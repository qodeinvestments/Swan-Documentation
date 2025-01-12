## Optimizations for Finnifty/Banknifty/Nifty : Individually and all 3 combined using EQ Curves
### Updated: 07.02.2023


###Introduction:We ran backtests using different Input Values on all 3 Indices to combine them.Individually each system has done well, but after combining the result improves in almost all cases and combinations. Also we have done the gap calculation from Open and Close because the results are different for both( This is because in the first five minutes the movement is big, 75% of the trades are different). This was the final result:

Note: In the excel file we can choose each system Individually to check the returns for each Index, and for combinations too.



#### Version 1:
Max Positions: 2


![image](https://user-images.githubusercontent.com/67407393/217734517-255244fe-9327-4f53-9a79-6fd18294e307.png)

Of these only the 20% gap system wasnt doing so well, because it was opening a lot more trades very early in the day.


12 Systems * 2 Positions = 24 Positions 

![image](https://user-images.githubusercontent.com/67407393/217734810-06936af3-f590-410e-a372-86390cdff9b8.png)


#### Version 2: 
Max Positions: 1
In this backtest instead of 20% gap we have taken 30% and instead of 2 positions 1.

![image](https://user-images.githubusercontent.com/67407393/217741158-a7e13e5c-543c-4da5-90b5-b8f01782735c.png)


12 Systems * 1 Position = 12 Positions 


![image](https://user-images.githubusercontent.com/67407393/217740955-235c8c5f-1e7e-4206-a448-0546c548f396.png)



#### Optimizations

We also ran optimizations to check for Max positions: Here are the results:After 2 positions the CAR/MDD Plateaus

![image](https://user-images.githubusercontent.com/67407393/217742230-9dc1db36-d7ea-48a1-877d-aa07e67fbd79.png)


![image](https://user-images.githubusercontent.com/67407393/217742303-b2e96734-1308-4750-9b0b-6d1642417942.png)


![image](https://user-images.githubusercontent.com/67407393/217742457-ab09a4a3-0336-4052-a9fb-ecd0abb287cc.png)

 
 #### Final Conclusion:
 
 The more systems the better the CAR/MDD and lesser curve fitting. The more Indices the better it gets.
 
 
