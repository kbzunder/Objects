from time import sleep
class Environment:
    def __init__(self, temperature, humidity):
        self.temperature = temperature
        self.humidity = humidity
    def get_temperature(self):
        return self.temperature

    def set_temperature(self, temperature):
        self.temperature = temperature

    def set_humidity(self, humidity):
        self.humidity = humidity

    def get_humidity(self):
        return self.humidity



class Conditioner:
    """ decreaes temperature by 1 degree if receives operate command from remote """
    def __init__(self, environment):
        self.environment = environment
    def operate(self):
        return self.environment.set_temperature(self.environment.get_temperature() - 1)

class Validator:
    """ checks environment temperature and humidity against user input,
    returns True if need to activate cooler, false otherwise"""
    def __init__(self, environment):
        self.environment = environment

    def validate(self, temperature, humidity):
        if temperature <  self.environment.get_temperature():
            return True
        else:
            return False


class Remote:
    def __init__(self, conditioner, validator, user_input):
        self.conditioner = conditioner
        self.validator = validator
        self.user_input = user_input


    def operate_conditioner(self, user_input):
        temp, hum = user_input

        while self.validator.validate(temp, hum):
            print(temp, hum)
            print("Validator returned True. Operating the conditioner...")
            self.conditioner.operate()
            sleep(2)

        print("Validator returned False. No need to operate.")

if __name__ == "__main__":
    environment = Environment(35, 50)
    conditioner = Conditioner(environment)
    validator = Validator(environment)
    remote = Remote(conditioner, validator, (25, 50))
    remote.operate_conditioner((25, 50))
    print(environment.get_temperature())
