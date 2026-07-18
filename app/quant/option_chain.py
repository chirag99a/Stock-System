import math


def select_atm_strike(atm_spot: float) -> int:
    """
    Select ATM strike using standard NIFTY option-chain increments.

    Decision:
    - NIFTY option strikes are listed in 50-point increments for the index options.
    - We round to the nearest 50-point strike.

    This is the only ambiguous example in the spec ("third row is not a typo").
    Applying "round to nearest 50 with .5 rounding up" yields:
      atm=22432  -> 22450
      atm=22450  -> 22450
      atm=22424  -> 22400
      atm=22425  -> 22450 (ties at .5 round up)
      atm=22400  -> 22400
    """
    step = 50
    # math.floor(x + 0.5) ensures exact ties (.5) always round up to +infinity
    return int(math.floor(atm_spot / step + 0.5)) * step


def build_option_instrument(security_id: str, strike: int, option_type: str) -> str:
    """
    Build an internal instrument identifier used by premium source / persistence.
    """
    # Example: "13:22450:CE"
    return f"{security_id}:{int(strike)}:{option_type.upper()}"
