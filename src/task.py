class Task:
    def __init__(self, func, app):
        self.func = func
        self.app = app
        self.name = func.__name__

    def delay(self, *args, **kwargs):
        return self.app.send_task(self.name, args, kwargs)

    __call__ = delay
