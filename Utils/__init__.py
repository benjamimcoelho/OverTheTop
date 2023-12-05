from enum import Enum


class OrderedEnum(Enum):
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


# Simple pairing function the return value is uniq based on both arguments
def pairing(a, b):
    res = 0.5 * (a + b) * (a + b + 1) + b
    if isinstance(a, int) and isinstance(b, int):
        return int(res)
    return res


class Scaling_Method(Enum):
    CONSTANT = 'CONSTANT',
    LINEAR = 'LINEAR'
    QUADRATIC = 'QUADRATIC'
    EXPONENCIAL = 'EXPONENCIAL'


class Scaling_Method_Library():
    __operations = {
        Scaling_Method.CONSTANT: lambda x, _: x,
        Scaling_Method.LINEAR: lambda x, y: x * y,
        Scaling_Method.QUADRATIC: lambda x, y: x * y * y,
        Scaling_Method.EXPONENCIAL: lambda x, y: x ** y
    }

    @staticmethod
    def get_operation(m):
        return Scaling_Method_Library.__operations[m]
