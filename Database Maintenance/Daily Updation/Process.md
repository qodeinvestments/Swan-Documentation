# Process followed to create the stock options database
## Adjustments
### A. BONUS
1. Ex. Bonus = 3:7
2. **Adjustment Factor** = (A+B)/B  
        &nbsp                 = 10/7
3. *New Strike Price = Old Strike Price / Adjustment Factor
4. Option Price = Old Price / Adjustment Factor
5. Lot Size = Old Lot Size * Adjustment Factor
6. Volume = Old Volume * Adjustment Factor


*If the revised strike prices after adjustment appear in decimal places, the strike shall be rounded off to the nearest tick size.i.e 0.05
### B. SPLITS
1. Face value splits from Rs.10/- to Rs.5/-.
2. **Adjustment Factor** = A/B  
                                                    = 10/5
3. *New Strike Price = Old Strike Price / Adjustment Factor
4. Option Price = Old Price / Adjustment Factor
5. Lot Size = Old Lot Size * Adjustment Factor
6. Volume = Old Volume * Adjustment Factor


*If the revised strike prices after adjustment appear in decimal places, the strike shall be rounded off to the nearest tick size.i.e 0.05
### C. RIGHTS ISSUE
1. Rights 5:21 @Premium Rs 3/- and F.V. is 2
2. A = 5  
   B = 21  
   C = 3  
   D = 2  
   X = Old Strike Price  
3. **Adjustment Factor** = ((B * X) + A * (C + D))/(A + B)
4. *New Strike Price = Old Strike Price / Adjustment Factor
5. Option Price = Old Price / Adjustment Factor
6. Lot Size = Old Lot Size * Adjustment Factor
7. Volume = Old Volume * Adjustment Factor


*If the revised strike prices after adjustment appear in decimal places, the strike shall be rounded off to the nearest tick size.i.e 0.05
### D. DIVIDENDS
1. For dividends, we need to check the difference between the strike prices on the day of adjustment and one day prior in the NSE bhavcopy.
2. If the difference matches with the dividend amount, then we will have to adjust the dividend for that stock.
