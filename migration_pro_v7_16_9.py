import sys, json, uuid, datetime, re, pandas as pd
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from sqlalchemy import create_engine, text


# --- 1. 执行引擎 (核心逻辑) ---
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
            if not tasks:
                self.finished.emit("⚠️ 该工作流中没有任务。")
                return
            for idx, task in enumerate(tasks):
                self.log(f"▶️ 正在执行步骤 {idx + 1}: {task['name']}")

                # 从连接管理器获取 URL
                src_url = self.conns.get(task.get('src_db'), task.get('src_db'))
                dest_url = self.conns.get(task.get('dest_db'), task.get('dest_db'))

                s_eng = create_engine(src_url)
                d_eng = create_engine(dest_url)
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
        self.setWindowTitle("数据同步工作流中心 Pro v7")
        self.resize(1280, 720)
        self.db_file = "workflows_config_v7.json"
        self.last_page_idx = 0  # 新增：记录进入日志页之前的页面
        data = self.load_data()
        self.workflows = data.get("workflows", {})
        self.connections = data.get("connections", {})  # 新增连接管理数据

        self.curr_wf_id = None
        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.init_dash_page()  # 0
        self.init_wf_page()  # 1
        self.init_task_page()  # 2
        self.init_log_page()  # 3

    def load_data(self):
        try:
            with open(self.db_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"workflows": {}, "connections": {}}

    def save_data(self):
        with open(self.db_file, 'w', encoding='utf-8') as f:
            json.dump({"workflows": self.workflows, "connections": self.connections}, f, indent=4)

    # --- 首页看板 ---
    def init_dash_page(self):
        page = QWidget();
        lay = QVBoxLayout(page)
        header = QHBoxLayout()
        header.addWidget(QLabel("<h2>🌊 工作流同步方案看板</h2>"))

        btn_conn = QPushButton("🔑 数据库连接管理")
        btn_conn.clicked.connect(self.manage_connections)
        header.addStretch();
        header.addWidget(btn_conn)
        lay.addLayout(header)

        self.wf_table = QTableWidget(0, 3)
        self.wf_table.setHorizontalHeaderLabels(["方案名称", "任务数", "操作"])
        self.wf_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.wf_table.setColumnWidth(2, 450)
        lay.addWidget(self.wf_table)

        btn = QPushButton("+ 新建同步方案");
        btn.setFixedHeight(45)
        btn.clicked.connect(self.new_wf);
        lay.addWidget(btn)
        self.stack.addWidget(page);
        self.refresh_dash()

    def refresh_dash(self):
        self.wf_table.setRowCount(len(self.workflows))
        for i, (wid, d) in enumerate(self.workflows.items()):
            self.wf_table.setItem(i, 0, QTableWidgetItem(d['name']))
            self.wf_table.setItem(i, 1, QTableWidgetItem(f"{len(d['tasks'])}个任务"))
            btns = QWidget();
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

    # --- 新增：数据库连接管理弹窗 ---
    # def manage_connections(self):
    #     dialog = QDialog(self);
    #     dialog.setWindowTitle("数据库连接池管理");
    #     dialog.resize(800, 500)
    #     dl = QVBoxLayout(dialog)
    #     table = QTableWidget(0, 3);
    #     table.setHorizontalHeaderLabels(["名称(别名)", "SQLAlchemy URL", "操作"])
    #     table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
    #     dl.addWidget(table)
    #
    #     def ref():
    #         table.setRowCount(len(self.connections))
    #         for i, (name, url) in enumerate(self.connections.items()):
    #             table.setItem(i, 0, QTableWidgetItem(name))
    #             table.setItem(i, 1, QTableWidgetItem(url))
    #             btn_del = QPushButton("删除");
    #             btn_del.clicked.connect(lambda _, n=name: (self.connections.pop(n), self.save_data(), ref()))
    #             table.setCellWidget(i, 2, btn_del)
    #
    #     btn_add = QPushButton("+ 添加新连接")
    #     btn_add.clicked.connect(lambda: self.add_conn_logic(ref))
    #     dl.addWidget(btn_add);
    #     ref();
    #     dialog.exec_()

    # def add_conn_logic(self, callback):
    #     name, ok1 = QInputDialog.getText(self, "连接名", "请输入连接别名(如: 生产库_202):")
    #     if not ok1 or not name: return
    #     url, ok2 = QInputDialog.getText(self, "连接地址", "请输入SQLAlchemy URL:")
    #     if ok2 and url:
    #         self.connections[name] = url
    #         self.save_data();
    #         callback()

    # --- 任务编辑页 (修改为下拉选择) ---
    def init_task_page(self):
        page = QWidget();
        lay = QVBoxLayout(page)
        splitter = QSplitter(Qt.Vertical)
        top = QWidget();
        tl = QVBoxLayout(top)
        self.t_name = QLineEdit();
        tl.addWidget(QLabel("任务名:"));
        tl.addWidget(self.t_name)

        # 修改点：将输入框改为下拉框
        tl.addWidget(QLabel("源数据库连接:"))
        self.t_src_combo = QComboBox();
        tl.addWidget(self.t_src_combo)
        tl.addWidget(QLabel("目标数据库连接:"))
        self.t_dest_combo = QComboBox();
        tl.addWidget(self.t_dest_combo)

        opt = QHBoxLayout()
        self.t_is_sql = QCheckBox("SQL模式");
        self.t_mode = QComboBox()
        self.t_mode.addItems(["append", "update", "upsert", "insert"])
        self.t_pk = QLineEdit();
        self.t_pk.setPlaceholderText("主键(目标列)")
        opt.addWidget(self.t_is_sql);
        opt.addWidget(QLabel("模式:"));
        opt.addWidget(self.t_mode);
        opt.addWidget(QLabel("主键:"));
        opt.addWidget(self.t_pk)
        tl.addLayout(opt)

        self.t_source = QTextEdit();
        self.t_source.setPlaceholderText("表名或SQL")
        splitter.addWidget(top);
        splitter.addWidget(self.t_source)
        lay.addWidget(splitter)
        self.t_dest_t = QLineEdit();
        lay.addWidget(QLabel("目标表:"));
        lay.addWidget(self.t_dest_t)
        self.t_map_table = QTableWidget(0, 2);
        self.t_map_table.setHorizontalHeaderLabels(["源", "目标"]);
        lay.addWidget(self.t_map_table)
        btns = QHBoxLayout();
        b_p = QPushButton("1.解析结构");
        b_p.clicked.connect(self.parse_cols)
        b_s = QPushButton("2.保存并返回");
        b_s.clicked.connect(self.save_task)
        btns.addWidget(b_p);
        btns.addWidget(b_s);
        lay.addLayout(btns)
        self.stack.addWidget(page)

    def open_task_edit(self, idx):
        self.curr_task_idx = idx
        # 刷新下拉列表
        for cb in [self.t_src_combo, self.t_dest_combo]:
            cb.clear();
            cb.addItems(self.connections.keys())

        if idx >= 0:
            t = self.workflows[self.curr_wf_id]['tasks'][idx]
            self.t_name.setText(t['name'])
            self.t_src_combo.setCurrentText(t.get('src_db', ''))
            self.t_dest_combo.setCurrentText(t.get('dest_db', ''))
            self.t_source.setPlainText(t['source'])
            self.t_dest_t.setText(t['dest_table'])
            self.t_pk.setText(t.get('sync_pk', ''))
            self.t_mode.setCurrentText(t['write_mode'])
            self.t_is_sql.setChecked(t.get('is_sql', False))
            self.t_map_table.setRowCount(len(t['mapping']))
            for r, (s, d) in enumerate(t['mapping'].items()):
                self.t_map_table.setItem(r, 0, QTableWidgetItem(s));
                self.t_map_table.setItem(r, 1, QTableWidgetItem(d))
        else:
            self.t_name.clear();
            self.t_source.clear();
            self.t_map_table.setRowCount(0)
        self.stack.setCurrentIndex(2)

    def save_task(self):
        m = {self.t_map_table.item(i, 0).text(): self.t_map_table.item(i, 1).text() for i in
             range(self.t_map_table.rowCount())}
        data = {
            "name": self.t_name.text(),
            "src_db": self.t_src_combo.currentText(),  # 存储别名
            "dest_db": self.t_dest_combo.currentText(),  # 存储别名
            "source": self.t_source.toPlainText(), "is_sql": self.t_is_sql.isChecked(),
            "dest_table": self.t_dest_t.text(), "write_mode": self.t_mode.currentText(),
            "sync_pk": self.t_pk.text(), "mapping": m
        }
        if self.curr_task_idx >= 0:
            self.workflows[self.curr_wf_id]['tasks'][self.curr_task_idx] = data
        else:
            self.workflows[self.curr_wf_id]['tasks'].append(data)
        self.save_data();
        self.refresh_task_list();
        self.stack.setCurrentIndex(1)

    def parse_cols(self):
        try:
            # 获取实际URL
            url = self.connections.get(self.t_src_combo.currentText())
            if not url: raise Exception("请先选择并配置源数据库连接")
            sql = self.t_source.toPlainText() if self.t_is_sql.isChecked() else f"SELECT * FROM `{self.t_source.toPlainText()}`"
            with create_engine(url).connect() as cn:
                cols = pd.read_sql(text(f"SELECT * FROM ({sql}) AS _m LIMIT 0"), cn).columns.tolist()
            self.t_map_table.setRowCount(len(cols))
            for i, c in enumerate(cols):
                self.t_map_table.setItem(i, 0, QTableWidgetItem(c));
                self.t_map_table.setItem(i, 1, QTableWidgetItem(c))
        except Exception as ex:
            QMessageBox.critical(self, "Error", str(ex))

    def test_conn(self, e):  # 此方法在当前逻辑下可按需保留或调整，目前连接管理页可处理
        pass

    # --- 其余逻辑保持不变 ---
    def rename_wf(self, wid):
        n, ok = QInputDialog.getText(self, "重命名", "新名称:", QLineEdit.Normal, self.workflows[wid]['name'])
        if ok and n: self.workflows[wid]['name'] = n; self.save_data(); self.refresh_dash()

    def delete_wf(self, wid):
        if QMessageBox.warning(self, "删除", "确认删除？", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            del self.workflows[wid];
            self.save_data();
            self.refresh_dash()

    def fast_run(self, wid):
        self.curr_wf_id = wid; self.run_wf()

    def delete_step(self, idx):
        if QMessageBox.question(self, "确认", "删除此步骤？") == QMessageBox.Yes:
            del self.workflows[self.curr_wf_id]['tasks'][idx];
            self.save_data();
            self.refresh_task_list()

    def new_wf(self):
        n, ok = QInputDialog.getText(self, "新建", "流名称:")
        if ok and n:
            id = str(uuid.uuid4());
            self.workflows[id] = {"name": n, "tasks": []}
            self.save_data();
            self.refresh_dash()

    def open_wf(self, wid):
        """打开工作流详情页"""
        self.curr_wf_id = wid
        self.refresh_task_list()
        self.stack.setCurrentIndex(1)  # 切换到任务序列页面

    # --- 数据库连接管理弹窗 (新增测试功能) ---
    def manage_connections(self):
        dialog = QDialog(self);
        dialog.setWindowTitle("数据库连接池管理");
        dialog.resize(900, 500)
        dl = QVBoxLayout(dialog)

        table = QTableWidget(0, 4);  # 增加一列用于测试
        table.setHorizontalHeaderLabels(["名称(别名)", "SQLAlchemy URL", "连接测试", "操作"])
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        dl.addWidget(table)

        def ref():
            table.setRowCount(len(self.connections))
            for i, (name, url) in enumerate(self.connections.items()):
                table.setItem(i, 0, QTableWidgetItem(name))
                table.setItem(i, 1, QTableWidgetItem(url))

                # 测试按钮
                btn_test = QPushButton("测试连接")
                btn_test.clicked.connect(lambda _, u=url: self.test_connection_url(u))
                table.setCellWidget(i, 2, btn_test)

                # 删除按钮
                btn_del = QPushButton("删除")
                btn_del.setStyleSheet("color: #dc3545;")
                btn_del.clicked.connect(lambda _, n=name: (self.connections.pop(n), self.save_data(), ref()))
                table.setCellWidget(i, 3, btn_del)

        btn_add = QPushButton("+ 添加新连接")
        btn_add.setFixedHeight(40)
        btn_add.setStyleSheet("background-color: #007bff; color: white; font-weight: bold;")
        btn_add.clicked.connect(lambda: self.add_conn_logic(ref))

        dl.addWidget(btn_add)
        ref()
        dialog.exec_()

    def add_conn_logic(self, callback):
        # 1. 输入名称
        name, ok1 = QInputDialog.getText(self, "连接名", "请输入连接别名(如: 生产库_202):")
        if not ok1 or not name: return

        # 2. 输入 URL
        url, ok2 = QInputDialog.getText(self, "连接地址", "请输入SQLAlchemy URL:", QLineEdit.Normal,
                                        self.connections.get(name, "mysql+pymysql://user:pass@host:3306/db"))
        if not ok2 or not url: return

        # 3. 询问是否在保存前测试
        res = QMessageBox.question(self, "测试连接", "是否在保存前测试该连接？", QMessageBox.Yes | QMessageBox.No)
        if res == QMessageBox.Yes:
            if not self.test_connection_url(url):
                return  # 测试失败则不关闭窗口，方便修改

        self.connections[name] = url
        self.save_data()
        callback()

    def test_connection_url(self, url):
        """通用的连接测试方法"""
        try:
            # 设置较短的超时时间，避免界面卡死
            engine = create_engine(url, connect_args={'connect_timeout': 5})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            QMessageBox.information(self, "成功", "✅ 数据库连接成功！")
            return True
        except Exception as e:
            QMessageBox.critical(self, "连接失败", f"❌ 无法连接到数据库:\n{str(e)}")
            return False

    def init_wf_page(self):
        page = QWidget()
        lay = QVBoxLayout(page)
        self.wf_label = QLabel("步骤管理")
        lay.addWidget(self.wf_label)

        # 修改：3列布局 [任务名, 排序, 操作]
        self.task_list = QTableWidget(0, 3)
        self.task_list.setHorizontalHeaderLabels(["任务名", "排序调整", "任务操作"])
        self.task_list.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.task_list.setColumnWidth(1, 150)  # 排序按钮宽度
        self.task_list.setColumnWidth(2, 200)  # 修改删除按钮宽度
        lay.addWidget(self.task_list)

        row = QHBoxLayout()
        b_add = QPushButton("添加步骤")
        b_add.clicked.connect(lambda: self.open_task_edit(-1))
        b_run = QPushButton("🚀 执行")
        b_run.clicked.connect(self.run_wf)
        b_back = QPushButton("返回首页")
        b_back.clicked.connect(lambda: self.stack.setCurrentIndex(0))
        row.addWidget(b_add);
        row.addWidget(b_run);
        row.addWidget(b_back)
        lay.addLayout(row)
        self.stack.addWidget(page)

    def refresh_task_list(self):
        wf = self.workflows[self.curr_wf_id]
        self.wf_label.setText(f"工作流: {wf['name']}")
        tasks = wf['tasks']
        self.task_list.setRowCount(len(tasks))

        for i, t in enumerate(tasks):
            # 0: 任务名
            self.task_list.setItem(i, 0, QTableWidgetItem(t['name']))

            # 1: 排序调整按钮 (上移/下移)
            sort_widget = QWidget()
            sl = QHBoxLayout(sort_widget);
            sl.setContentsMargins(0, 0, 0, 0)
            btn_up = QPushButton("↑");
            btn_down = QPushButton("↓")
            btn_up.setFixedWidth(40);
            btn_down.setFixedWidth(40)

            # 边界禁用
            if i == 0: btn_up.setEnabled(False)
            if i == len(tasks) - 1: btn_down.setEnabled(False)

            btn_up.clicked.connect(lambda _, x=i: self.move_step(x, -1))
            btn_down.clicked.connect(lambda _, x=i: self.move_step(x, 1))

            sl.addWidget(btn_up);
            sl.addWidget(btn_down)
            self.task_list.setCellWidget(i, 1, sort_widget)

            # 2: 任务操作 (修改/删除)
            btns = QWidget()
            bl = QHBoxLayout(btns);
            bl.setContentsMargins(0, 0, 0, 0)
            be = QPushButton("修改");
            bd = QPushButton("删除")
            be.clicked.connect(lambda _, x=i: self.open_task_edit(x))
            bd.clicked.connect(lambda _, x=i: self.delete_step(x))
            bl.addWidget(be);
            bl.addWidget(bd)
            self.task_list.setCellWidget(i, 2, btns)

    def move_step(self, index, direction):
        """
        index: 当前行索引
        direction: -1 表示上移, 1 表示下移
        """
        tasks = self.workflows[self.curr_wf_id]['tasks']
        new_index = index + direction

        # 再次确认边界，防止越界
        if 0 <= new_index < len(tasks):
            # 交换数组元素
            tasks[index], tasks[new_index] = tasks[new_index], tasks[index]
            # 保存数据并刷新界面
            self.save_data()
            self.refresh_task_list()

    def run_wf(self):
        # 1. 记录当前页面索引，以便后续返回
        self.last_page_idx = self.stack.currentIndex()

        # 2. 切换到日志页
        self.stack.setCurrentIndex(3)
        self.log_txt.clear()

        self.worker = WorkflowThread(self.workflows[self.curr_wf_id], self.connections)
        self.worker.log_update.connect(self.log_txt.append)

        # 3. 弹窗结束后不做页面跳转，让用户留在日志页查看细节
        self.worker.finished.connect(lambda m: QMessageBox.information(self, "状态", m))
        self.worker.start()

    def init_log_page(self):
        page = QWidget()
        lay = QVBoxLayout(page)
        self.log_txt = QTextEdit()
        self.log_txt.setReadOnly(True)
        self.log_txt.setStyleSheet("background-color: #1e1e1e; color: #61afef; font-family: 'Consolas';")
        lay.addWidget(self.log_txt)

        b = QPushButton("关闭日志并返回")
        # 修改点：跳转到记录的上一个页面，而不是固定的索引
        b.clicked.connect(lambda: self.stack.setCurrentIndex(self.last_page_idx))

        lay.addWidget(b)
        self.stack.addWidget(page)

def print_logo():
    logo = """
    #################################################
    #                                               #
    #    ____        _        ____      _     _     #
    #   |  _ \  __ _| |_ __ _| __ ) _ _(_) __| | __ _ #
    #   | | | |/ _` | __/ _` |  _ \| '__| |/ _` |/ _` |#
    #   | |_| | (_| | || (_| | |_) | |  | | (_| | (_| |#
    #   |____/ \__,_|\__\__,_|____/|_|  |_|\__,_|\__, |#
    #                                            |___/  #
    #            --- DataBridge Pro v7 ---          #
    #################################################
    """
    print(logo)

if __name__ == "__main__":
    print_logo() # 启动时打印 Logo
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MigrationApp();
    win.show();
    sys.exit(app.exec_())