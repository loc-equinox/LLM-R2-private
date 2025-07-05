from difflib import SequenceMatcher
import re

def get_plan_cost_string(plan_text):
    """
    Extracts the top-level cost from a PostgreSQL EXPLAIN plan and returns it as a string.
    Format: "start_cost..end_cost"
    Returns None if no cost is found.
    """
    cost_match = re.search(r'\(cost=(\d+\.\d+)\.\.(\d+\.\d+)', plan_text)
    if cost_match:
        return f"{cost_match.group(1)}..{cost_match.group(2)}"
    return None

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
    '''
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
    '''
    c1 = get_plan_cost_string(plan1)
    c2 = get_plan_cost_string(plan2)
    print(c1, c2)
    return SequenceMatcher(None, c1, c2).ratio()

def is_cost_lower(cost_str1, cost_str2):
    """
    Compare two query plan cost strings and determine if the second cost is lower than the first.
    
    Args:
        cost_str1: First cost string in format "start1..end1"
        cost_str2: Second cost string in format "start2..end2"
        
    Returns:
        bool: True if cost_str2 represents a lower cost than cost_str1, False otherwise
    """
    def parse_cost(cost_str):
        """Helper function to parse cost string into numeric values"""
        start, end = cost_str.split('..')
        return float(start), float(end)
    
    # Parse both cost strings
    start1, end1 = parse_cost(cost_str1)
    start2, end2 = parse_cost(cost_str2)
    return end2 < end1


def plan_is_effective(plan1, plan2):
    """Calculate whether the plan2 has a lower cost than plan1"""
    c1 = get_plan_cost_string(plan1)
    c2 = get_plan_cost_string(plan2)
    return is_cost_lower(c1, c2)
