"""A single rounded pace trend, fitted jointly across slope bands.

pace(g) = minimum + left * min(0, (g - optimum) / 30)**2
                  + right * max(0, (g - optimum) / 30)**2

Nonnegative coefficients prevent extra extrema. The two parabolas share a
horizontal tangent at the optimum. This is a constrained empirical trend,
not a physiological law; observed values are kept separately by the caller.
"""
import itertools
import math
import statistics

VERSION = "asymmetric-quadratic-v1"


def evaluate(model, grade):
    """Evaluate only inside the fitted support; never invent unsupported extremes."""
    if not model or not isinstance(grade, (float, int)) or not math.isfinite(grade):
        return None
    if not model['x_min'] <= grade <= model['x_max']:
        return None
    delta = (grade - model['optimum']) / 30
    return model['minimum'] + model['left' if delta < 0 else 'right'] * delta**2


def _solve(matrix, vector):
    """Small pivoted system (at most three coefficients), no numerical dependency."""
    rows = [list(row) + [value] for row, value in zip(matrix, vector)]
    for col in range(len(rows)):
        pivot = max(range(col, len(rows)), key=lambda i: abs(rows[i][col]))
        if abs(rows[pivot][col]) < 1e-12:
            return None
        rows[col], rows[pivot] = rows[pivot], rows[col]
        divisor = rows[col][col]
        rows[col] = [v / divisor for v in rows[col]]
        for i in range(len(rows)):
            if i != col:
                factor = rows[i][col]
                rows[i] = [a - factor*b for a, b in zip(rows[i], rows[col])]
    return [row[-1] for row in rows]


def _nonnegative_fit(features, values, weights):
    gram = [[sum(w*f[i]*f[j] for f, w in zip(features, weights)) for j in range(3)] for i in range(3)]
    rhs = [sum(w*f[i]*v for f, v, w in zip(features, values, weights)) for i in range(3)]
    best, best_loss = None, math.inf
    # Active-set enumeration is exact for this tiny nonnegative least-squares fit.
    for size in range(1, 4):
        for active in itertools.combinations(range(3), size):
            solution = _solve([[gram[i][j] for j in active] for i in active], [rhs[i] for i in active])
            if solution is None or any(v < 0 for v in solution):
                continue
            coefficients = [0., 0., 0.]
            for i, value in zip(active, solution):
                coefficients[i] = value
            loss = sum(w*(sum(c*x for c, x in zip(coefficients, f))-v)**2
                       for f, v, w in zip(features, values, weights))
            if loss < best_loss:
                best, best_loss = coefficients, loss
    return best, best_loss


def fit(points):
    """Fit reliable personal cells and collective fallback cells together.

    The optimum is constrained to -25..0% as a product modeling choice. Relative
    errors avoid giving slow extreme slopes disproportionate influence. Two
    robust reweightings limit the influence of isolated atypical measurements.
    At least three distinct slope bands spanning 10 percentage points are needed.
    """
    points = sorted([p for p in points if all(isinstance(p.get(k), (float, int))
                    and math.isfinite(p[k]) for k in ('x', 'pace'))
                    and p['pace'] > 0 and -45 <= p['x'] <= 45], key=lambda p: p['x'])
    if len({p['x'] for p in points}) < 3 or points[-1]['x'] - points[0]['x'] < 10:
        return None
    # Normalize pace so the fit is invariant to units and any target-time factor.
    scale = statistics.median(p['pace'] for p in points)
    values = [p['pace']/scale for p in points]
    floor = min(values)*.5
    base_weights = [max(.1, min(3., p.get('weight', 1.)))/v**2 for p, v in zip(points, values)]
    weights = base_weights[:]
    best = None
    for iteration in range(3):
        best_loss = math.inf
        for step in range(51):
            optimum = -25 + step*.5
            features = [[1., min(0., (p['x']-optimum)/30)**2, max(0., (p['x']-optimum)/30)**2] for p in points]
            coefficients, loss = _nonnegative_fit(features, [v-floor for v in values], weights)
            if coefficients is not None and loss < best_loss:
                best_loss = loss
                best = dict(version=VERSION, minimum=(coefficients[0]+floor)*scale,
                            left=coefficients[1]*scale, right=coefficients[2]*scale,
                            optimum=optimum, x_min=points[0]['x'], x_max=points[-1]['x'])
        if best is None:
            return None
        residuals = [abs(evaluate(best, p['x'])/p['pace']-1) for p in points]
        threshold = max(.08, 1.5*statistics.median(residuals))
        weights = [w*min(1., threshold/max(r, 1e-12)) for w, r in zip(base_weights, residuals)]
    return best
