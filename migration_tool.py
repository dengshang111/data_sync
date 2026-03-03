import sys
import json
import os
import pandas as pd
from datetime import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QLineEdit, QPushButton,
                             QTableWidget, QTableWidgetItem, QComboBox,
                             QProgressBar, QMessageBox, QStackedWidget, QHeaderView,
                             QTextEdit, QRadioButton, QButtonGroup, QListWidget)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from sqlalchemy import create_engine, inspect, text

# 配置文件路径
DB_HISTORY_FILE = "migration_history.json"


# 1. 后台同步线程
class SyncThread(QThread):
    progress_update = pyqtSignal(int)
    finished = pyqtSignal(str)

    def __init__(self, src_url, dest_url, mapping, final_sql, dest_table):
        super().__init__()
        self.src_url = src_url
        self.dest_url = dest_url
        self.mapping = mapping
        self.final_sql = final_sql
        self.dest_table = dest_table

    def run(self):
        try:
            src_engine = create_engine(self.src_url)
            dest_engine = create_engine(self.dest_url)

            # 获取总数
            with src_engine.connect() as conn:
                count_sql = text(f"SELECT COUNT(*) FROM ({self.final_sql}) AS _t")
                total = conn.execute(count_sql).scalar()

            if total == 0:
                self.finished.emit("查询无数据。")
                return

            chunk_size = 2000
            processed = 0
            with src_engine.connect() as conn:
                for chunk_df in pd.read_sql(text(self.final_sql), conn, chunksize=chunk_size):
                    # 仅保留映射列并重命名
                    cols = list(self.mapping.keys())
                    chunk_df = chunk_df[cols].rename(columns=self.mapping)
                    chunk_df.to_sql(self.dest_table, dest_engine, if_exists='append', index=False)

                    processed += len(chunk_df)
                    self.progress_update.emit(int((processed / total) * 100))

            self.finished.emit(f"✅ 同步成功！迁移 {processed} 条。")
        except Exception as e:
            self.finished.emit(f"❌ 失败: {str(e)}")


# 2. 主程序
class MigrationApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("数据迁移管理系统 v3.0")
        self.resize(1000, 800)
        self.history_data = self.load_all_history()
        self.current_config_id = None  # 用于标记是修改还是新建

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.init_home_page()  # Index 0
        self.init_config_page()  # Index 1
        self.init_mapping_page()  # Index 2

    # --- 数据持久化 ---
    def load_all_history(self):
        if os.path.exists(DB_HISTORY_FILE):
            with open(DB_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []

    def save_to_history(self, config):
        if self.current_config_id is not None:
            self.history_data[self.current_config_id] = config
        else:
            config['create_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.history_data.append(config)

        with open(DB_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.history_data, f, indent=4, ensure_ascii=False)
        self.refresh_history_list()

    # --- 页面 0: 初始首页 ---
    def init_home_page(self):
        page = QWidget()
        layout = QVBoxLayout()

        layout.addWidget(QLabel("<h2>数据迁移历史记录</h2>"))
        self.history_list = QListWidget()
        self.refresh_history_list()
        layout.addWidget(self.history_list)

        btn_hbox = QHBoxLayout()
        btn_new = QPushButton("+ 新建迁移配置")
        btn_new.setStyleSheet("height: 40px; background-color: #28a745; color: white;")
        btn_new.clicked.connect(self.go_to_new_config)

        btn_edit = QPushButton("📂 加载/编辑选中项")
        btn_edit.clicked.connect(self.load_selected_history)

        btn_hbox.addWidget(btn_new);
        btn_hbox.addWidget(btn_edit)
        layout.addLayout(btn_hbox)
        page.setLayout(layout)
        self.stack.addWidget(page)

    def refresh_history_list(self):
        self.history_list.clear()
        for i, item in enumerate(self.history_data):
            display = f"[{item['create_time']}] {item['name']} | 目标: {item['dest_t']}"
            self.history_list.addItem(display)

    # --- 页面 1: 配置页 ---
    def init_config_page(self):
        page = QWidget()
        layout = QVBoxLayout()

        self.cfg_name = QLineEdit();
        self.cfg_name.setPlaceholderText("给这次配置起个名字")
        layout.addWidget(QLabel("配置名称:"));
        layout.addWidget(self.cfg_name)

        self.src_db = QLineEdit();
        layout.addWidget(QLabel("源库连接（mysql+pymysql://root:password@127.0.0.1:3306/old_db）:"));
        layout.addWidget(self.src_db)
        self.dest_db = QLineEdit();
        layout.addWidget(QLabel("目标库连接（mysql+pymysql://root:password@127.0.0.1:3306/new_db）:"));
        layout.addWidget(self.dest_db)

        # 模式切换
        mode_box = QHBoxLayout()
        self.radio_table = QRadioButton("单表模式");
        self.radio_sql = QRadioButton("SQL 模式")
        self.radio_table.setChecked(True)
        mode_box.addWidget(self.radio_table);
        mode_box.addWidget(self.radio_sql)
        layout.addLayout(mode_box)

        self.src_t_input = QLineEdit();
        self.src_t_input.setPlaceholderText("输入源表名")
        self.src_sql_input = QTextEdit();
        self.src_sql_input.setPlaceholderText("输入 SELECT 语句")
        self.src_sql_input.setHidden(True)

        self.radio_table.toggled.connect(lambda: self.toggle_mode(True))
        self.radio_sql.toggled.connect(lambda: self.toggle_mode(False))

        layout.addWidget(QLabel("源数据设置:"));
        layout.addWidget(self.src_t_input);
        layout.addWidget(self.src_sql_input)

        self.dest_t = QLineEdit();
        layout.addWidget(QLabel("目标写入表名:"));
        layout.addWidget(self.dest_t)

        btn_box = QHBoxLayout()
        btn_cancel = QPushButton("返回主页");
        btn_cancel.clicked.connect(lambda: self.stack.setCurrentIndex(0))
        btn_next = QPushButton("解析结构并下一步 ▶");
        btn_next.clicked.connect(self.goto_mapping)
        btn_box.addWidget(btn_cancel);
        btn_box.addWidget(btn_next)
        layout.addLayout(btn_box)

        page.setLayout(layout)
        self.stack.addWidget(page)

    def toggle_mode(self, is_table):
        self.src_t_input.setVisible(is_table)
        self.src_sql_input.setVisible(not is_table)

    # --- 页面 2: 映射页 ---
    def init_mapping_page(self):
        page = QWidget()
        layout = QVBoxLayout()
        self.m_table = QTableWidget(0, 2)
        self.m_table.setHorizontalHeaderLabels(["源字段", "目标字段"])
        self.m_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.m_table)
        self.p_bar = QProgressBar();
        layout.addWidget(self.p_bar)

        ctrl_box = QHBoxLayout()
        btn_back = QPushButton("◀ 上一步");
        btn_back.clicked.connect(lambda: self.stack.setCurrentIndex(1))
        self.btn_run = QPushButton("🚀 开始执行并保存配置");
        self.btn_run.clicked.connect(self.do_migration)
        ctrl_box.addWidget(btn_back);
        ctrl_box.addWidget(self.btn_run)
        layout.addLayout(ctrl_box)
        page.setLayout(layout)
        self.stack.addWidget(page)

    # --- 逻辑控制 ---
    def go_to_new_config(self):
        self.current_config_id = None
        self.cfg_name.clear();
        self.src_db.clear();
        self.dest_db.clear()
        self.src_t_input.clear();
        self.src_sql_input.clear();
        self.dest_t.clear()
        self.current_mapping = {}
        self.stack.setCurrentIndex(1)

    def load_selected_history(self):
        idx = self.history_list.currentRow()
        if idx < 0: return
        self.current_config_id = idx
        item = self.history_data[idx]
        self.cfg_name.setText(item['name'])
        self.src_db.setText(item['src_db'])
        self.dest_db.setText(item['dest_db'])
        self.dest_t.setText(item['dest_t'])
        if item.get('mode') == 'sql':
            self.radio_sql.setChecked(True)
            self.src_sql_input.setPlainText(item['src_sql'])
        else:
            self.radio_table.setChecked(True)
            self.src_t_input.setText(item['src_t'])
        self.current_mapping = item.get('mapping', {})
        self.stack.setCurrentIndex(1)

    def goto_mapping(self):
        try:
            s_url = self.src_db.text().strip()
            # 自动构建 SQL
            if self.radio_table.isChecked():
                self.final_sql = f"SELECT * FROM `{self.src_t_input.text().strip()}`"
            else:
                self.final_sql = self.src_sql_input.toPlainText().strip()

            s_eng = create_engine(s_url)
            d_eng = create_engine(self.dest_db.text().strip())

            with s_eng.connect() as conn:
                df_meta = pd.read_sql(text(f"SELECT * FROM ({self.final_sql}) AS _meta LIMIT 0"), conn)
                s_cols = df_meta.columns.tolist()

            d_cols = [c['name'] for c in inspect(d_eng).get_columns(self.dest_t.text().strip())]

            self.m_table.setRowCount(len(s_cols))
            for i, col in enumerate(s_cols):
                self.m_table.setItem(i, 0, QTableWidgetItem(col))
                cb = QComboBox();
                cb.addItems(["--忽略--"] + d_cols)
                if col in self.current_mapping:
                    cb.setCurrentText(self.current_mapping[col])
                elif col in d_cols:
                    cb.setCurrentText(col)
                self.m_table.setCellWidget(i, 1, cb)

            self.stack.setCurrentIndex(2)
        except Exception as e:
            QMessageBox.critical(self, "解析错误", str(e))

    def do_migration(self):
        mapping = {}
        for i in range(self.m_table.rowCount()):
            s = self.m_table.item(i, 0).text()
            d = self.m_table.cellWidget(i, 1).currentText()
            if d != "--忽略--": mapping[s] = d

        # 保存当前配置
        config = {
            "name": self.cfg_name.text() or "未命名配置",
            "src_db": self.src_db.text(), "dest_db": self.dest_db.text(),
            "mode": "table" if self.radio_table.isChecked() else "sql",
            "src_t": self.src_t_input.text(), "src_sql": self.src_sql_input.toPlainText(),
            "dest_t": self.dest_t.text(), "mapping": mapping
        }
        self.save_to_history(config)

        self.btn_run.setEnabled(False)
        self.worker = SyncThread(self.src_db.text(), self.dest_db.text(), mapping, self.final_sql, self.dest_t.text())
        self.worker.progress_update.connect(self.p_bar.setValue)
        self.worker.finished.connect(
            lambda m: [QMessageBox.information(self, "完成", m), self.btn_run.setEnabled(True)])
        self.worker.start()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MigrationApp()
    win.show()
    sys.exit(app.exec_())