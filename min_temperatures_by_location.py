# min temperatures based on each location
# fields: location, date, type, data, x, y, z, w
# command in Canopy (nagivate to the project root first): !python min_temperatures_by_location.py ./assets/data/year1800temperatures.csv > ./target/results.txt


from mrjob.job import MRJob

class MRMinTemperature(MRJob):

    def makeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(',')
        if (type == 'TMIN'):
            temperature = self.makeFahrenheit(data)
            yield location, temperature

    def reducer(self, location, temperatures):
        yield location, min(temperatures)

if __name__ == '__main__':
    MRMinTemperature.run()
