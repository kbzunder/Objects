from time import sleep
class Environment:
    def __init__(self, temperature, humidity):
        self.temperature = temperature
        self.humidity = humidity
    def get_temperature(self):
        print(f"Environment: current temperature: {self.temperature}")
        return self.temperature

    def set_temperature(self, temperature):
        self.temperature = temperature

    def set_humidity(self, humidity):
        self.humidity = humidity

    def get_humidity(self):
        return self.humidity

class Validator:
    def __init__(self, environment):
        self.environment = environment

    def validate(self, temperature, humidity):
        return temperature <  self.environment.get_temperature()

class Conditioner:
    def __init__(self, environment):
        self.environment = environment

    def operate(self):
        return self.environment.set_temperature(self.environment.get_temperature() - 1)

class User:
    def __init__(self,remote):
       self.remote = remote

    def set_user_input(self, user_input):
        self.remote.set_user_input(user_input)

class Controller:
    def __init__(self, remote, validator, conditioner):
        self.remote = remote
        self.validator = validator
        self.conditioner = conditioner

    def operate_conditioner(self):
        temp, hum = self.remote.get_user_input()
        while self.validator.validate(temp, hum):
            print("Validator returned True. Operating the conditioner...")
            self.conditioner.operate()
            sleep(2)

class Remote:
    def __init__(self):
        default_settings = (26, 35)
        self.user_input = default_settings

    def set_user_input(self, user_input):
        self.user_input = user_input

    def get_user_input(self):
        return self.user_input


if __name__ == "__main__":
    environment = Environment(33, 35)
    remote = Remote()
    user = User(remote)
    user.set_user_input((25, 50))
    validator = Validator(environment)
    conditioner = Conditioner(environment)
    controller = Controller(remote, validator,conditioner)
    controller.operate_conditioner()
    print(environment.get_temperature())



