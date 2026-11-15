"""入口脚本：python3 -I run_server.py

-I 隔离模式下 `python -m app.server` 无法从 cwd 找到包，
因此由本脚本显式注入项目根目录，再调用 app.server.main()。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.server import main  # noqa: E402

if __name__ == "__main__":
    main()
