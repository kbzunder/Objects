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


class Conditioner:
    """ decreases temperature by 1 degree if receives operate command from remote """
    """ #TODO: 1. make Validator (Thermostat) as part of the Conditioner """
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
        return temperature <  self.environment.get_temperature()
        

class Remote:
    """ #TODO: 2. make the Remote as proxy of the Conditioner """
    def __init__(self, conditioner, validator):
        self.conditioner = conditioner
        self.validator = validator
        default_settings = (26, 35)
        self.user_input = default_settings

    def set_user_input(self, user_input):
        self.user_input = user_input
    
    def operate_conditioner(self):
        temp, hum = self.user_input

        """ TODO: 3. Just for an excercise (in our simple version) Remote can check the Validator
            and also operate the Conditioner, but...
            probably it is more correct/realistic to create a Controller class which receives commands from the Remote, 
            sets the Validator and activates the Conditioner """
        while self.validator.validate(temp, hum):
            print("Validator returned True. Operating the conditioner...")
            self.conditioner.operate()
            sleep(2)

        print("Remote: Validator returned False. No need to operate.")


""" the user """
def client_code(remote) -> None:
    # client (User)
    user_input = (25, 50)
    remote.set_user_input(user_input)
    remote.operate_conditioner()


if __name__ == "__main__":
    """ Process starter (Main) """
    environment = Environment(35, 50)
    conditioner = Conditioner(environment)
    validator = Validator(environment)
    remote = Remote(conditioner, validator)
    client_code(remote)
    
    # result:
    print(environment.get_temperature())
