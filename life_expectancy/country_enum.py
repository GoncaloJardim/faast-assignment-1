"""Country Enum class to validate country parse."""
from enum import auto
from strenum import StrEnum

class Country(StrEnum):
    """Class to validate input in parser for region/country."""
    BG = auto()
    BE = auto()
    CH = auto()
    AT = auto()
    CY = auto()
    CZ = auto()
    EL = auto()
    EE = auto()
    ES = auto()
    DK = auto()
    FI = auto()
    FR = auto()
    HR = auto()
    HU = auto()
    IS = auto()
    IT = auto()
    LI = auto()
    LT = auto()
    LU = auto()
    LV = auto()
    MT = auto()
    NL = auto()
    NO = auto()
    PL = auto()
    PT = auto()
    RO = auto()
    SE = auto()
    SI = auto()
    SK = auto()
    DE = auto()
    DE_TOT = auto()
    AL = auto()
    IE = auto()
    ME = auto()
    MK = auto()
    RS = auto()
    AM = auto()
    AZ = auto()
    GE = auto()
    TR = auto()
    UA = auto()
    BY = auto()
    UK = auto()
    XK = auto()
    FX = auto()
    MD = auto()
    SM = auto()
    RU = auto()

    @classmethod
    def _missing_(cls, value):
        value = value.lower()
        for member in cls:
            if member.value == value:
                return member
        return None
