# 🌊 DataBridge-Pro 
> **高效、稳定的跨库数据同步可视化工作台**

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org)
[![Framework](https://img.shields.io/badge/framework-PyQt5-orange.svg)](https://www.riverbankcomputing.com/software/pyqt/)
[![ORM](https://img.shields.io/badge/ORM-SQLAlchemy-red.svg)](https://www.sqlalchemy.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)

---

这是一个基于 **PyQt5** 和 **SQLAlchemy** 开发的轻量级可视化数据迁移与同步工具。支持多数据库连接管理、自定义 SQL 抽取、字段映射以及任务流的灵活排序执行。

## ✨ 核心特性

* **可视化看板**：一目了然地查看所有同步方案及其任务数量。
* **连接池管理**：集中管理 SQLAlchemy 数据库 URL，支持一键连接测试。
* **灵活同步模式**：
    * `append`: 增量追加。
    * `update`: 根据主键更新已有记录。
    * `upsert`: MySQL 独有的 `ON DUPLICATE KEY UPDATE`。
    * `insert`: 直接执行原生 SQL 脚本。
* **任务流编排**：支持步骤的添加、删除及**上下移动排序**。
* **实时日志**：内置控制台实时输出同步进度与报错信息。

## 🚀 快速开始

### 1. 环境准备
确保已安装 Python 3.8+，然后克隆仓库并安装依赖：

```bash
git clone [https://github.com/你的用户名/仓库名.git](https://github.com/你的用户名/仓库名.git)
cd 仓库名
pip install -r requirements.txt
```
### 2. 运行程序
```Bash
python main.py
```
### 3. 使用流程
    点击 🔑 数据库连接管理 配置源库和目标库地址。
    
    新建一个 同步方案。
    
    在方案中 添加步骤，选择连接、编写 SQL 或表名。
    
    点击 解析结构 自动生成字段映射，确认无误后保存。
    
    点击 ▶ 运行 开始同步。

### 🛠️ 技术栈

    GUI 框架: PyQt5 (Fusion 风格)
    
    数据库引擎: SQLAlchemy (支持 MySQL, PostgreSQL, Oracle 等)
    
    数据处理: Pandas (流式分块读取，内存友好)
    
    配置文件: JSON (本地持久化存储)

### 结构示意图
    
    用户界面 (PyQt5) <---> 逻辑层 (MigrationApp) <---> 核心执行引擎 (WorkflowThread)
                                    |
                            +-------+-------+
                            |               |
                      数据提取 (Pandas)   配置存储 (JSON)

### 执行打包
```bash
python build.py
```