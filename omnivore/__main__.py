from .app import Blueprint
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv('./.env')
    omni = Blueprint()
    omni.run()

