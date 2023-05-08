# Calendar Spreads
A calendar spread is an options strategy established by simultaneously entering a long and short position on the same underlying asset but with different Expiry dates. 
In a typical calendar spread, one would buy a longer-term contract and go short a nearer-term option with the same strike price.


## Backtest:

### Data Used - 
- Banknifty weekly - I,II,III,IV and monthly - I,II.
- Expiry Dates.


## Optimisations:
### 1. Calendar Spread:
- Short current Expiry contract and Long next expiry Contract.
- Performed for Weekly and Monthly Contracts.
- Performed on Only Calls, Only Puts and Call and Puts combined.
- This had flat to negative [results].

### 2. Reverse Calendar Spread:

- Short Next Expiry contract and Long Current expiry Contract.
- Performed for Weekly and Monthly Contracts.
- Performed on Only Calls, Only Puts and Call and Puts combined.
- This had Better [results].
- Chose this for further testing.

### 3. 1st Week and 3rd Week:
- Short 1st Week Expiry contract and Long 3rd Week expiry Contract.
- Performed reverse as well.
- Performed on Only Calls, Only Puts and Call and Puts combined.

### 4. 1st Week and 4th week:
- Short 1st Week Expiry contract and Long 4t Week expiry Contract.
- Performed reverse as well.
- Performed on Only Calls, Only Puts and Call and Puts combined.

### 5. 2nd Week and 3rd Week:
- Short 2nd Week Expiry contract and Long 3rd Week expiry Contract.
- Performed reverse as well.
- Performed on Only Calls, Only Puts and Call and Puts combined.

### 6. 2nd Week and 4th Week:
- Short 2nd Week Expiry contract and Long 4t Week expiry Contract.
- Performed reverse as well.
- Performed on Only Calls, Only Puts and Call and Puts combined.

### 7. Budget day optimisations:
- Tested Calendars and reverse calendars specifically during budget month and week.

### 8. Picking Longer term contract based on shorter term contract premium:
- Longer term contract strike was selected based on the premium of the shorter term contract.
- Longer term contract with strike closest to a certain percentage of the shorter term contract was selected.
- This Percentage ranged from 25% to 200%.
- Performed for all combinations of calendar spreads.

### 9. Contract Filter:
- Backtest was performed again with a contract filter >=1000.

### 10. Opening on each day of week:
- Separate backtests were performed where Positions are opened on Each day of the week. 

## Position Sizing:
- Number of Shares.
