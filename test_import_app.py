import importlib, traceback, sys
try:
    importlib.import_module("app")
    print("import app: OK")
except Exception:
    traceback.print_exc()
    sys.exit(1)
