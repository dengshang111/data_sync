import ctypes
import os
import sys
import json
import uuid
import datetime
import re
from urllib.parse import quote_plus

import pandas as pd
import time

from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QPropertyAnimation, QSequentialAnimationGroup
from sqlalchemy import create_engine, text
from sqlalchemy.pool.impl import NullPool

# 1. 尝试导入 pyi_splash (仅在打包环境下生效)
try:
    import pyi_splash
except ImportError:
    pyi_splash = None


# --- 1. 执行引擎 ---
class WorkflowThread(QThread):
    log_update = pyqtSignal(str)
    finished = pyqtSignal(str)

    def __init__(self, workflow_data, connections):
        super().__init__()
        self.wf = workflow_data
        self.conns = connections

    def log(self, msg):
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_update.emit(f"[{now}] {msg}")

    def run(self):
        try:
            tasks = self.wf.get('tasks', [])
            for idx, task in enumerate(tasks):
                self.log(f"▶️ 执行步骤 {idx + 1}: {task['name']}")

                # 【关键修复】直接从传入的最新 connections 字典中获取
                # 这样即使在工作流运行间隙修改了 URL，下一步也会读到新的
                src_alias = task.get('src_db')
                dest_alias = task.get('dest_db')

                src_url = self.conns.get(src_alias)
                dest_url = self.conns.get(dest_alias)

                if not src_url or not dest_url:
                    raise Exception(f"未找到连接: {src_alias} -> {dest_alias}")

                # 使用 pool_pre_ping 确保连接是新鲜的
                s_eng = create_engine(src_url, poolclass=NullPool)
                d_eng = create_engine(dest_url, poolclass=NullPool)
                query = task['source'] if task['is_sql'] else f"SELECT * FROM `{task['source']}`"
                mode = task.get('write_mode', 'append')
                pk_cols = [c.strip() for c in re.split('[,;，；]', task.get('sync_pk', '')) if c.strip()]

                if mode == 'insert':
                    with d_eng.begin() as conn:
                        conn.execute(text(query))
                else:
                    with s_eng.connect() as conn:
                        for chunk in pd.read_sql(text(query), conn, chunksize=1000):
                            chunk = chunk[list(task['mapping'].keys())].rename(columns=task['mapping'])
                            if mode == 'append':
                                chunk.to_sql(task['dest_table'], d_eng, if_exists='append', index=False)
                            else:
                                self.process_upsert(chunk, d_eng, task['dest_table'], pk_cols, mode)
                self.log(f"✅ 步骤 {task['name']} 同步完成")
                # 执行完一步后，显式销毁引擎释放连接，防止占用旧配置
                s_eng.dispose()
                d_eng.dispose()
            self.finished.emit(f"🏁 工作流 [{self.wf['name']}] 全部执行成功！")
        except Exception as e:
            self.log(f"❌ 错误: {str(e)}")
            self.finished.emit("执行失败")

    def process_upsert(self, df, engine, table, pk_cols, mode):
        with engine.begin() as conn:
            for _, row in df.iterrows():
                params = row.to_dict()
                where = " AND ".join([f"`{c}`=:{c}" for c in pk_cols])
                upd_cols = [c for c in params.keys() if c not in pk_cols]
                set_c = ", ".join([f"`{c}`=:{c}" for c in upd_cols])
                if mode == 'update':
                    sql = text(f"UPDATE `{table}` SET {set_c} WHERE {where}")
                else:
                    cols, vals = ", ".join([f"`{k}`" for k in params.keys()]), ", ".join(
                        [f":{k}" for k in params.keys()])
                    upd_stmt = ", ".join([f"`{k}`=VALUES(`{k}`)" for k in upd_cols])
                    sql = text(f"INSERT INTO `{table}` ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE {upd_stmt}")
                conn.execute(sql, params)


# --- 2. 主界面 ---
class MigrationApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DataBridge-Pro")
        self.resize(1280, 720)
        self.db_file = "workflows_config.json"
        self.last_page_idx = 0

        data = self.load_data()
        self.workflows = data.get("workflows", {})
        self.connections = data.get("connections", {})

        self.curr_wf_id = None
        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.init_dash_page()  # 0
        self.init_wf_page()  # 1
        self.init_task_page()  # 2
        self.init_log_page()  # 3
        self.set_app_icon()

    def set_app_icon(self):
        if hasattr(sys, '_MEIPASS'):
            icon_path = os.path.join(sys._MEIPASS, "app_icon.ico")
        else:
            icon_path = "app_icon.ico"
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

    def load_data(self):
        try:
            with open(self.db_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"workflows": {}, "connections": {}}

    def save_data(self):
        with open(self.db_file, 'w', encoding='utf-8') as f:
            json.dump({"workflows": self.workflows, "connections": self.connections}, f, indent=4)

    # --- 看板页 ---
    def init_dash_page(self):
        page = QWidget()
        lay = QVBoxLayout(page)
        header = QHBoxLayout()
        header.addWidget(QLabel("<h2>🌊 工作流同步方案看板</h2>"))
        btn_conn = QPushButton("🔑 数据库连接管理")
        btn_conn.clicked.connect(self.manage_connections)
        header.addStretch()
        header.addWidget(btn_conn)
        lay.addLayout(header)

        self.wf_table = QTableWidget(0, 3)
        self.wf_table.setHorizontalHeaderLabels(["方案名称", "任务数", "操作"])
        self.wf_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.wf_table.setColumnWidth(2, 450)
        lay.addWidget(self.wf_table)

        btn = QPushButton("+ 新建同步方案")
        btn.setFixedHeight(45)
        btn.clicked.connect(self.new_wf)
        lay.addWidget(btn)
        self.stack.addWidget(page)
        self.refresh_dash()

    def refresh_dash(self):
        self.wf_table.setRowCount(len(self.workflows))
        for i, (wid, d) in enumerate(self.workflows.items()):
            self.wf_table.setItem(i, 0, QTableWidgetItem(d['name']))
            self.wf_table.setItem(i, 1, QTableWidgetItem(f"{len(d['tasks'])}个任务"))
            btns = QWidget()
            bl = QHBoxLayout(btns);
            bl.setContentsMargins(5, 2, 5, 2)
            b_run = QPushButton("▶ 运行");
            b_run.setStyleSheet("background-color: #28a745; color: white;")
            b_run.clicked.connect(lambda _, id=wid: self.fast_run(id))
            b_manage = QPushButton("配置步骤");
            b_manage.clicked.connect(lambda _, id=wid: self.open_wf(id))
            b_edit = QPushButton("改名");
            b_edit.clicked.connect(lambda _, id=wid: self.rename_wf(id))
            b_del = QPushButton("删除");
            b_del.setStyleSheet("color: #dc3545;")
            b_del.clicked.connect(lambda _, id=wid: self.delete_wf(id))
            for b in [b_run, b_manage, b_edit, b_del]: bl.addWidget(b)
            self.wf_table.setCellWidget(i, 2, btns)

    def test_connection_url(self, url):
        try:
            engine = create_engine(url, connect_args={'connect_timeout': 5})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            QMessageBox.information(self, "成功", "✅ 连接成功！")
            return True
        except Exception as e:
            QMessageBox.critical(self, "失败", f"❌ 连接失败:\n{str(e)}")
            return False

    # --- 任务列表与排序 ---
    def init_wf_page(self):
        page = QWidget()
        lay = QVBoxLayout(page)
        self.wf_label = QLabel("步骤管理")
        lay.addWidget(self.wf_label)
        self.task_list = QTableWidget(0, 3)
        self.task_list.setHorizontalHeaderLabels(["任务名", "排序调整", "操作"])
        self.task_list.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        lay.addWidget(self.task_list)
        row = QHBoxLayout()
        b_add = QPushButton("添加步骤");
        b_add.clicked.connect(lambda: self.open_task_edit(-1))
        b_run = QPushButton("🚀 执行");
        b_run.clicked.connect(self.run_wf)
        b_back = QPushButton("返回首页");
        b_back.clicked.connect(lambda: self.stack.setCurrentIndex(0))
        for b in [b_add, b_run, b_back]: row.addWidget(b)
        lay.addLayout(row)
        self.stack.addWidget(page)

    def refresh_task_list(self):
        tasks = self.workflows[self.curr_wf_id]['tasks']
        self.task_list.setRowCount(len(tasks))
        for i, t in enumerate(tasks):
            self.task_list.setItem(i, 0, QTableWidgetItem(t['name']))
            sort_w = QWidget();
            sl = QHBoxLayout(sort_w);
            sl.setContentsMargins(0, 0, 0, 0)
            bu = QPushButton("↑");
            bd = QPushButton("↓")
            bu.clicked.connect(lambda _, x=i: self.move_step(x, -1))
            bd.clicked.connect(lambda _, x=i: self.move_step(x, 1))
            bu.setEnabled(i > 0);
            bd.setEnabled(i < len(tasks) - 1)
            sl.addWidget(bu);
            sl.addWidget(bd)
            self.task_list.setCellWidget(i, 1, sort_w)

            ops = QWidget();
            ol = QHBoxLayout(ops);
            ol.setContentsMargins(0, 0, 0, 0)
            be = QPushButton("修改");
            bdel = QPushButton("删除")
            be.clicked.connect(lambda _, x=i: self.open_task_edit(x))
            bdel.clicked.connect(lambda _, x=i: self.delete_step(x))
            ol.addWidget(be);
            ol.addWidget(bdel)
            self.task_list.setCellWidget(i, 2, ops)

    def move_step(self, index, direction):
        tasks = self.workflows[self.curr_wf_id]['tasks']
        new_idx = index + direction
        tasks[index], tasks[new_idx] = tasks[new_idx], tasks[index]
        self.save_data();
        self.refresh_task_list()

    # --- 任务编辑页 ---
    def init_task_page(self):
        page = QWidget();
        lay = QVBoxLayout(page)
        self.t_name = QLineEdit();
        lay.addWidget(QLabel("任务名:"));
        lay.addWidget(self.t_name)

        gl = QGridLayout()
        self.t_src_combo = QComboBox();
        gl.addWidget(QLabel("源库:"), 0, 0);
        gl.addWidget(self.t_src_combo, 0, 1)
        self.t_dest_combo = QComboBox();
        gl.addWidget(QLabel("目标库:"), 1, 0);
        gl.addWidget(self.t_dest_combo, 1, 1)
        lay.addLayout(gl)

        opt = QHBoxLayout()
        self.t_is_sql = QCheckBox("SQL模式");
        self.t_mode = QComboBox();
        self.t_mode.addItems(["append", "update", "upsert", "insert"])
        self.t_pk = QLineEdit();
        self.t_pk.setPlaceholderText("主键")
        opt.addWidget(self.t_is_sql);
        opt.addWidget(self.t_mode);
        opt.addWidget(self.t_pk)
        lay.addLayout(opt)

        self.t_source = QTextEdit();
        lay.addWidget(self.t_source)
        self.t_dest_t = QLineEdit();
        lay.addWidget(QLabel("目标表:"));
        lay.addWidget(self.t_dest_t)
        self.t_map_table = QTableWidget(0, 2);
        self.t_map_table.setHorizontalHeaderLabels(["源", "目标"]);
        lay.addWidget(self.t_map_table)

        btns = QHBoxLayout()
        bp = QPushButton("1.解析结构");
        bp.clicked.connect(self.parse_cols)
        bs = QPushButton("2.保存返回");
        bs.clicked.connect(self.save_task)
        btns.addWidget(bp);
        btns.addWidget(bs);
        lay.addLayout(btns)
        self.stack.addWidget(page)

    def open_task_edit(self, idx):
        self.curr_task_idx = idx
        for cb in [self.t_src_combo, self.t_dest_combo]:
            cb.clear();
            cb.addItems(self.connections.keys())
        if idx >= 0:
            t = self.workflows[self.curr_wf_id]['tasks'][idx]
            self.t_name.setText(t['name']);
            self.t_src_combo.setCurrentText(t.get('src_db', ''))
            self.t_dest_combo.setCurrentText(t.get('dest_db', ''));
            self.t_source.setPlainText(t['source'])
            self.t_dest_t.setText(t['dest_table']);
            self.t_pk.setText(t.get('sync_pk', ''))
            self.t_mode.setCurrentText(t['write_mode']);
            self.t_is_sql.setChecked(t.get('is_sql', False))
            self.t_map_table.setRowCount(len(t['mapping']))
            for r, (s, d) in enumerate(t['mapping'].items()):
                self.t_map_table.setItem(r, 0, QTableWidgetItem(s));
                self.t_map_table.setItem(r, 1, QTableWidgetItem(d))
        self.stack.setCurrentIndex(2)

    def parse_cols(self):
        try:
            url = self.connections.get(self.t_src_combo.currentText())
            sql = self.t_source.toPlainText() if self.t_is_sql.isChecked() else f"SELECT * FROM `{self.t_source.toPlainText()}` LIMIT 0"
            with create_engine(url).connect() as cn:
                cols = pd.read_sql(text(sql), cn).columns.tolist()
            self.t_map_table.setRowCount(len(cols))
            for i, c in enumerate(cols):
                self.t_map_table.setItem(i, 0, QTableWidgetItem(c));
                self.t_map_table.setItem(i, 1, QTableWidgetItem(c))
        except Exception as e:
            QMessageBox.critical(self, "解析错误", str(e))

    def save_task(self):
        m = {self.t_map_table.item(i, 0).text(): self.t_map_table.item(i, 1).text() for i in
             range(self.t_map_table.rowCount())}
        data = {"name": self.t_name.text(), "src_db": self.t_src_combo.currentText(),
                "dest_db": self.t_dest_combo.currentText(),
                "source": self.t_source.toPlainText(), "is_sql": self.t_is_sql.isChecked(),
                "dest_table": self.t_dest_t.text(),
                "write_mode": self.t_mode.currentText(), "sync_pk": self.t_pk.text(), "mapping": m}
        if self.curr_task_idx >= 0:
            self.workflows[self.curr_wf_id]['tasks'][self.curr_task_idx] = data
        else:
            self.workflows[self.curr_wf_id]['tasks'].append(data)
        self.save_data();
        self.refresh_task_list();
        self.stack.setCurrentIndex(1)

    # --- 日志页 ---
    def init_log_page(self):
        page = QWidget();
        lay = QVBoxLayout(page)
        self.log_txt = QTextEdit();
        self.log_txt.setReadOnly(True)
        self.log_txt.setStyleSheet("background-color: #1e1e1e; color: #61afef; font-family: 'Consolas';")
        lay.addWidget(self.log_txt)
        b = QPushButton("关闭日志并返回")
        b.clicked.connect(lambda: self.stack.setCurrentIndex(self.last_page_idx))
        lay.addWidget(b);
        self.stack.addWidget(page)

    def run_wf(self):
        self.last_page_idx = self.stack.currentIndex()
        self.stack.setCurrentIndex(3);
        self.log_txt.clear()
        self.worker = WorkflowThread(self.workflows[self.curr_wf_id], self.connections)
        self.worker.log_update.connect(self.log_txt.append)
        self.worker.finished.connect(lambda m: QMessageBox.information(self, "状态", m))
        self.worker.start()

    def open_wf(self, wid):
        self.curr_wf_id = wid; self.refresh_task_list(); self.stack.setCurrentIndex(1)

    def fast_run(self, wid):
        self.curr_wf_id = wid; self.run_wf()

    def new_wf(self):
        n, ok = QInputDialog.getText(self, "新建", "流名称:")
        if ok and n: id = str(uuid.uuid4()); self.workflows[id] = {"name": n,
                                                                   "tasks": []}; self.save_data(); self.refresh_dash()

    def rename_wf(self, wid):
        n, ok = QInputDialog.getText(self, "重命名", "新名称:", QLineEdit.Normal, self.workflows[wid]['name'])
        if ok and n: self.workflows[wid]['name'] = n; self.save_data(); self.refresh_dash()

    def delete_wf(self, wid):
        if QMessageBox.warning(self, "删除", "确认删除？", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            del self.workflows[wid];
            self.save_data();
            self.refresh_dash()

    def delete_step(self, idx):
        if QMessageBox.question(self, "确认", "删除此步骤？") == QMessageBox.Yes:
            del self.workflows[self.curr_wf_id]['tasks'][idx];
            self.save_data();
            self.refresh_task_list()

    # --- 数据库连接管理 (UI 升级版) ---
    def manage_connections(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("数据库连接池管理")
        dialog.resize(1100, 500)
        dl = QVBoxLayout(dialog)

        table = QTableWidget(0, 5)
        table.setHorizontalHeaderLabels(["名称(别名)", "SQLAlchemy URL", "测试", "编辑", "删除"])
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        dl.addWidget(table)

        def ref():
            table.setRowCount(len(self.connections))
            for i, (name, url) in enumerate(self.connections.items()):
                table.setItem(i, 0, QTableWidgetItem(name))
                table.setItem(i, 1, QTableWidgetItem(url))

                btn_t = QPushButton("测试");
                btn_t.clicked.connect(lambda _, u=url: self.test_connection_url(u))
                table.setCellWidget(i, 2, btn_t)

                btn_e = QPushButton("修改")
                # 点击修改时，同时传入当前的名称和URL
                btn_e.clicked.connect(lambda _, n=name, u=url: self.add_conn_logic(ref, True, n, u))
                table.setCellWidget(i, 3, btn_edit := btn_e)

                btn_d = QPushButton("删除");
                btn_d.setStyleSheet("color: #dc3545;")
                btn_d.clicked.connect(lambda _, n=name: (self.connections.pop(n), self.save_data(), ref()))
                table.setCellWidget(i, 4, btn_d)

        btn_add = QPushButton("+ 添加新连接")
        btn_add.setFixedHeight(40);
        btn_add.setStyleSheet("background-color: #007bff; color: white;")
        btn_add.clicked.connect(lambda: self.add_conn_logic(ref))
        dl.addWidget(btn_add)
        ref()
        dialog.exec_()

    # --- 数据库连接管理逻辑 (增强字符处理版) ---
    def add_conn_logic(self, callback, edit_mode=False, old_name="", old_url=""):
        diag = QDialog(self)
        diag.setWindowTitle("编辑连接配置" if edit_mode else "新建连接配置")
        diag.setMinimumWidth(800)
        form = QFormLayout(diag)

        name_input = QLineEdit(old_name)
        url_input = QLineEdit(old_url if edit_mode else "mysql+pymysql://user:pass@host:3306/db")

        form.addRow("连接别名:", name_input)
        form.addRow("数据库 URL:", url_input)

        # 添加小贴士提示用户直接输入原密码
        tip = QLabel("<font color='gray'>提示：密码含 @ 或 : 等特殊字符请直接输入，系统会自动转义</font>")
        form.addRow(tip)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(diag.accept)
        btns.rejected.connect(diag.reject)
        form.addRow(btns)

        if diag.exec_() == QDialog.Accepted:
            new_name = name_input.text().strip()
            raw_url = url_input.text().strip()

            if not new_name or not raw_url:
                QMessageBox.warning(self, "错误", "名称和 URL 不能为空")
                return

            # --- 关键：特殊字符转义处理 ---
            processed_url = raw_url
            try:
                # 匹配模式: 协议://用户名:密码@主机地址
                if "://" in raw_url and "@" in raw_url:
                    protocol, rest = raw_url.split("://", 1)
                    # 找到最后一个 @ 符号，其前面是 用户名:密码
                    auth_part, host_part = rest.rsplit("@", 1)

                    if ":" in auth_part:
                        user, password = auth_part.split(":", 1)
                        # 仅对密码部分进行 URL 编码
                        safe_password = quote_plus(password)
                        processed_url = f"{protocol}://{user}:{safe_password}@{host_part}"
            except Exception as e:
                print(f"URL 自动转义失败: {e}")
                # 如果解析失败，则保留原样由 SQLAlchemy 报错

            # 测试处理后的连接
            if QMessageBox.question(self, "测试", "保存前测试连接？") == QMessageBox.Yes:
                if not self.test_connection_url(processed_url):
                    return

            # 级联更新逻辑
            if edit_mode and old_name != new_name:
                self.connections.pop(old_name, None)
                for wf in self.workflows.values():
                    for task in wf.get('tasks', []):
                        if task.get('src_db') == old_name: task['src_db'] = new_name
                        if task.get('dest_db') == old_name: task['dest_db'] = new_name

            # 保存处理后的安全 URL
            self.connections[new_name] = processed_url
            self.save_data()
            callback()


if __name__ == "__main__":
    # 1. 任务栏 ID 必须在 QApplication 之前设置
    if sys.platform == 'win32':
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID('ds.dataBridge.v7')
        except:
            pass

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # 全局图标
    app.setWindowIcon(QIcon("app_icon.ico"))

    # 2. 检查启动封面
    if pyi_splash:
        pyi_splash.close()

    win = MigrationApp()
    win.show()
    sys.exit(app.exec_())