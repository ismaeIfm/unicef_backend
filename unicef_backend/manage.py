from click import echo

from flask_script import Manager
from unicef_backend import create_app

app = create_app('development')

manager = Manager(app)

if __name__ == '__main__':
    manager.run()
