from datetime import datetime, timedelta

# Loan tier rules (modifiable)
LOAN_TIERS = [
    {'min': 100_000, 'max': 500_000, 'months': 1},
    {'min': 500_001, 'max': 1_000_000, 'months': 3},
    {'min': 1_000_001, 'max': 1_500_000, 'months': 6},
    {'min': 1_500_001, 'max': 2_000_000, 'months': 8},
]

FIXED_INTEREST_RATE = 0.10
FIXED_PENALTY_PER_DAY = 1000

def calculate_interest(principal):
    return principal * FIXED_INTEREST_RATE

def calculate_total(principal):
    return principal + calculate_interest(principal)

def get_repayment_period(amount):
    for tier in LOAN_TIERS:
        if tier['min'] <= amount <= tier['max']:
            return tier['months']
    return 1

def calculate_next_due(start_date, period_months):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return (start + timedelta(days=30*period_months)).strftime("%Y-%m-%d")

def calculate_penalty(days_late):
    return days_late * FIXED_PENALTY_PER_DAY
