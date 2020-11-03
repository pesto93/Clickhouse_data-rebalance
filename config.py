from dotenv import load_dotenv
import os

load_dotenv()


user = os.getenv("SSH_USER")
hosts = os.getenv("SSH_HOST")
port = int(os.getenv("SSH_PORT"))
proxy_hosts = os.getenv("SSH_PROXY_HOST")
proxy_port = int(os.getenv("SSH_PROXY_PORT"))
db = os.getenv("CH_DATABASE")
connection_str = os.getenv("CONN")

newline = '\n'
version_number = '2'
cluster = '{cluster}'
