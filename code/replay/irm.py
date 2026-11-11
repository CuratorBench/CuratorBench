"""Integer-exact Python port of Morpho's AdaptiveCurveIRM + Blue accrual math."""

WAD = 10**18
SECONDS_PER_YEAR = 365 * 24 * 3600

CURVE_STEEPNESS = 4 * WAD
ADJUSTMENT_SPEED = 50 * WAD // SECONDS_PER_YEAR
TARGET_UTILIZATION = 9 * WAD // 10
INITIAL_RATE_AT_TARGET = WAD * 4 // 100 // SECONDS_PER_YEAR
MIN_RATE_AT_TARGET = WAD // 1000 // SECONDS_PER_YEAR
MAX_RATE_AT_TARGET = 2 * WAD // SECONDS_PER_YEAR

LN_2_INT = 693_147_180_559_945_309
LN_WEI_INT = -41_446_531_673_892_822_312
WEXP_UPPER_BOUND = 93_859_467_695_000_404_319
WEXP_UPPER_VALUE = (
    57_716_089_161_558_943_949_701_069_502_944_508_345_128 * WAD
    + 422_502_756_744_429_568
)

VIRTUAL_SHARES = 10**6
VIRTUAL_ASSETS = 1


def sdiv(a, b):
    """Solidity int256 division."""
    q = abs(a) // abs(b)
    return -q if (a < 0) != (b < 0) else q


def w_mul_to_zero(x, y):
    return sdiv(x * y, WAD)


def w_div_to_zero(x, y):
    return sdiv(x * WAD, y)


def bound(x, low, high):
    """UtilsLib.bound: max(min(x, high), low)."""
    return max(min(x, high), low)


def mul_div_down(x, y, d):
    return (x * y) // d


def mul_div_up(x, y, d):
    return (x * y + (d - 1)) // d


def w_mul_down(x, y):
    return mul_div_down(x, y, WAD)


def w_div_down(x, y):
    return mul_div_down(x, WAD, y)


def w_div_up(x, y):
    return mul_div_up(x, WAD, y)


def w_taylor_compounded(x, n):
    """e^(x*n) - 1, first three Taylor terms (Blue MathLib)."""
    first = x * n
    second = mul_div_down(first, first, 2 * WAD)
    third = mul_div_down(second, first, 3 * WAD)
    return first + second + third


def to_shares_down(assets, total_assets, total_shares):
    return mul_div_down(assets, total_shares + VIRTUAL_SHARES,
                        total_assets + VIRTUAL_ASSETS)


def to_assets_down(shares, total_assets, total_shares):
    return mul_div_down(shares, total_assets + VIRTUAL_ASSETS,
                        total_shares + VIRTUAL_SHARES)


def to_shares_up(assets, total_assets, total_shares):
    return mul_div_up(assets, total_shares + VIRTUAL_SHARES,
                      total_assets + VIRTUAL_ASSETS)


def to_assets_up(shares, total_assets, total_shares):
    return mul_div_up(shares, total_assets + VIRTUAL_ASSETS,
                      total_shares + VIRTUAL_SHARES)


def w_exp(x):
    """ExpLib.wExp: 2^q * e^r decomposition, 2nd-order Taylor for e^r."""
    if x < LN_WEI_INT:
        return 0
    if x >= WEXP_UPPER_BOUND:
        return WEXP_UPPER_VALUE
    rounding = -(LN_2_INT // 2) if x < 0 else LN_2_INT // 2
    q = sdiv(x + rounding, LN_2_INT)
    r = x - q * LN_2_INT
    exp_r = WAD + r + (r * r) // WAD // 2
    return exp_r << q if q >= 0 else exp_r >> -q


def _curve(rate_at_target, err):
    """r = ((C-1)*err + 1) * rateAtTarget for err >= 0, ((1-1/C)*err + 1) * rateAtTarget for err < 0."""
    if err < 0:
        coeff = WAD - w_div_to_zero(WAD, CURVE_STEEPNESS)
    else:
        coeff = CURVE_STEEPNESS - WAD
    return w_mul_to_zero(w_mul_to_zero(coeff, err) + WAD, rate_at_target)


def _new_rate_at_target(start_rate_at_target, linear_adaptation):
    return bound(
        w_mul_to_zero(start_rate_at_target, w_exp(linear_adaptation)),
        MIN_RATE_AT_TARGET, MAX_RATE_AT_TARGET,
    )


def borrow_rate(start_rate_at_target, total_supply_assets,
                total_borrow_assets, elapsed):
    """AdaptiveCurveIrm._borrowRate."""
    utilization = (
        w_div_down(total_borrow_assets, total_supply_assets)
        if total_supply_assets > 0 else 0
    )
    err_norm = (WAD - TARGET_UTILIZATION
                if utilization > TARGET_UTILIZATION else TARGET_UTILIZATION)
    err = w_div_to_zero(utilization - TARGET_UTILIZATION, err_norm)

    if start_rate_at_target == 0:
        avg_rate_at_target = INITIAL_RATE_AT_TARGET
        end_rate_at_target = INITIAL_RATE_AT_TARGET
    else:
        speed = w_mul_to_zero(ADJUSTMENT_SPEED, err)
        linear_adaptation = speed * elapsed
        if linear_adaptation == 0:
            avg_rate_at_target = start_rate_at_target
            end_rate_at_target = start_rate_at_target
        else:
            end_rate_at_target = _new_rate_at_target(
                start_rate_at_target, linear_adaptation)
            mid_rate_at_target = _new_rate_at_target(
                start_rate_at_target, sdiv(linear_adaptation, 2))
            avg_rate_at_target = sdiv(
                start_rate_at_target + end_rate_at_target
                + 2 * mid_rate_at_target, 4)

    return _curve(avg_rate_at_target, err), end_rate_at_target


def accrue_interest(tsa, tss, tba, tbs, rate_at_target, elapsed, fee=0):
    """Morpho._accrueInterest for an AdaptiveCurveIRM market."""
    avg_rate, end_rate_at_target = borrow_rate(rate_at_target, tsa, tba,
                                               elapsed)
    interest = w_mul_down(tba, w_taylor_compounded(avg_rate, elapsed))
    tba += interest
    tsa += interest
    fee_shares = 0
    if fee != 0:
        fee_amount = w_mul_down(interest, fee)
        fee_shares = to_shares_down(fee_amount, tsa - fee_amount, tss)
        tss += fee_shares
    return tsa, tss, tba, tbs, end_rate_at_target, avg_rate, interest, fee_shares
