import PyInstaller.__main__
import os

# 定义你的主程序文件名
main_script = "main.py"
# 定义输出的可执行文件名
exe_name = "DataBridge-Pro"
# 图标文件（可选，如果没有 .ico 文件可以删掉 --icon 这一行）
icon_path = "app_icon.ico" 

params = [
    main_script,
    '--onefile',            # 打包成单个 exe 文件
    '--windowed',           # 运行时不显示命令行窗口 (GUI 程序必备)
    f'--name={exe_name}',   # 指定生成的文件名
    '--clean',              # 打包前清理临时文件
    # '--icon=' + icon_path # 如果你有图标，取消这一行的注释
]

if __name__ == "__main__":
    print(f"🚀 正在开始打包 {exe_name}...")
    PyInstaller.__main__.run(params)
    print(f"✅ 打包完成！请在 'dist' 文件夹中查看 {exe_name}.exe")