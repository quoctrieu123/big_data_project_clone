from dotenv import load_dotenv
from script.utils import load_environment_variables
load_dotenv()
env_vars = load_environment_variables()
print(type(env_vars.get("KAFKA_BROKERS")))