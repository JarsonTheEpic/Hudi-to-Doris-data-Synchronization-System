import os
import time
import platform
from datetime import datetime

LOG_FILE = "system_log.txt"


# -------------------------
# 工具函数
# -------------------------
def log(message):
    """写入日志文件"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {message}\n")


def clear():
    os.system("cls" if os.name == "nt" else "clear")


# -------------------------
# 系统信息
# -------------------------
def system_info():
    print("\n===== 系统信息 =====")
    print(f"系统类型: {platform.system()}")
    print(f"系统版本: {platform.version()}")
    print(f"机器类型: {platform.machine()}")
    print(f"处理器: {platform.processor()}")
    print(f"当前时间: {datetime.now()}")

    log("查看系统信息")


# -------------------------
# 文件工具
# -------------------------
def list_files():
    print("\n===== 当前目录文件 =====")
    files = os.listdir(".")
    for f in files:
        print(" -", f)

    log("列出文件")


def create_file():
    name = input("输入文件名: ")
    with open(name, "w", encoding="utf-8") as f:
        f.write("Hello from Python tool!\n")

    print(f"文件 {name} 创建成功")
    log(f"创建文件 {name}")


def write_file():
    name = input("文件名: ")
    content = input("输入内容: ")

    with open(name, "a", encoding="utf-8") as f:
        f.write(content + "\n")

    print("写入成功")
    log(f"写入文件 {name}")


# -------------------------
# 模拟任务
# -------------------------
def fake_task():
    print("\n开始模拟任务...")
    for i in range(1, 6):
        print(f"进度: {i * 20}%")
        time.sleep(0.5)

    print("任务完成！")
    log("执行模拟任务")


# -------------------------
# 菜单
# -------------------------
def menu():
    print("\n========================")
    print("  MINI PY SYSTEM TOOL")
    print("========================")
    print("1. 查看系统信息")
    print("2. 列出文件")
    print("3. 创建文件")
    print("4. 写入文件")
    print("5. 模拟任务")
    print("6. 查看日志")
    print("0. 退出")


def show_log():
    print("\n===== 日志内容 =====")
    if not os.path.exists(LOG_FILE):
        print("暂无日志")
        return

    with open(LOG_FILE, "r", encoding="utf-8") as f:
        print(f.read())


# -------------------------
# 主程序
# -------------------------
def main():
    log("程序启动")

    while True:
        menu()
        choice = input("请选择: ")

        clear()

        if choice == "1":
            system_info()
        elif choice == "2":
            list_files()
        elif choice == "3":
            create_file()
        elif choice == "4":
            write_file()
        elif choice == "5":
            fake_task()
        elif choice == "6":
            show_log()
        elif choice == "0":
            log("程序退出")
            print("再见！")
            break
        else:
            print("无效选择")


if __name__ == "__main__":
    main()