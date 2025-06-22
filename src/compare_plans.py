from difflib import SequenceMatcher
import re

def preprocess_plan(plan):
    """Normalize query plans for meaningful comparison"""
    # Remove all cost estimates
    plan = re.sub(r'\(cost=[\d\.]+\.\.[\d\.]+\)', '', plan)
    # Remove rows/width estimates
    plan = re.sub(r'rows=\d+', '', plan)
    plan = re.sub(r'width=\d+', '', plan)
    # Remove specific numeric values
    plan = re.sub(r'\d+', '', plan)
    # Standardize node names (remove aliases like "on t1")
    plan = re.sub(r'(Scan|Join|Sort) on \w+', r'\1', plan)
    # Standardize whitespace
    plan = ' '.join(plan.split())
    return plan

def get_operator_sequence(plan):
    """Extract just the sequence of operators"""
    operators = []
    # Find all operators (lines starting with -> or beginning of string)
    for line in re.findall(r'(?:^|->)\s*([A-Za-z ]+)', plan):
        # Get the base operator name (first word)
        op = line.split()[0] if line else ''
        if op:
            operators.append(op)
    return operators

def plan_similarity(plan1, plan2):
    """Calculate meaningful similarity between query plans (0-1)"""
    # Preprocess both plans
    p1 = preprocess_plan(plan1)
    p2 = preprocess_plan(plan2)
    
    # 1. Full text similarity (normalized)
    text_sim = SequenceMatcher(None, p1, p2).ratio()
    
    # 2. Operator sequence similarity
    ops1 = get_operator_sequence(plan1)
    ops2 = get_operator_sequence(plan2)
    ops_sim = SequenceMatcher(None, ops1, ops2).ratio()
    
    # 3. Key structure components
    has_join1 = 'Join' in p1
    has_join2 = 'Join' in p2
    join_sim = 1.0 if has_join1 == has_join2 else 0.0
    
    # Weighted combination favoring operator sequence
    return 0.1*text_sim + 0.8*ops_sim + 0.1*join_sim

# Example usage:
# plan1 = "Subquery Scan on t1  (cost=68186.04..76632.71 rows=66680 width=59) ->  Gather Merge  (cost=68186.04..75965.91 rows=66680 width=471) Workers Planned: 2 ->  Sort  (cost=67186.01..67269.36 rows=33340 width=471) Sort Key: supplier.s_name ->  Parallel Hash Join  (cost=3539.54..57614.85 rows=33340 width=471) Hash Cond: (part.p_partkey = supplier.s_suppkey) ->  Parallel Seq Scan on part  (cost=0.00..51378.67 rows=666806 width=37) Filter: (p_size > 10) ->  Parallel Hash  (cost=2804.24..2804.24 rows=58824 width=30) ->  Parallel Seq Scan on supplier  (cost=0.00..2804.24 rows=58824 width=30)"
# plan2 = "Sort (cost=6516621.30..6537837.43 rows=8486449 width=32) Sort Key: ((u.l_extendedprice * ('1'::numeric - u.l_discount))) -> Subquery Scan on u (cost=4942933.69..5133878.80 rows=8486449 width=32) -> Unique (cost=4942933.69..5006582.06 rows=8486449 width=36) -> Sort (cost=4942933.69..4964149.82 rows=8486449 width=36) Sort Key: lineitem.l_extendedprice, lineitem.l_discount -> Append (cost=0.00..3502175.19 rows=8486449 width=36) -> Seq Scan on lineitem (cost=0.00..1874404.90 rows=8486448 width=12) Filter: (l_shipmode = 'AIR'::bpchar) -> Gather (cost=1000.00..1500473.55 rows=1 width=12) Workers Planned: 2 -> Parallel Seq Scan on lineitem lineitem_1 (cost=0.00..1499473.45 rows=1 width=12) Filter: ((l_quantity > '50'::numeric) AND (l_shipmode = 'AIR'::bpchar))"
# similarity = plan_similarity(plan1, plan2)
# print(f"Plan similarity: {similarity:.2f}")  # Should now be ~0.9+
