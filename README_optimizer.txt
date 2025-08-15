
Superflex Auction Draft Optimizer (Notebook)
===========================================
Saved: 2025-08-08T04:20:10.001932

Files created:
- /mnt/data/auction_superflex_optimizer_2025.ipynb
- data/template_players.csv
- data/template_kdst.csv

Next steps:
1) Download the notebook and open it locally (or in a cloud notebook with internet access if you plan to use the web fetchers).
2) Export your data:
   - FantasyPros Auction Calculator for a 12-team, Half-PPR, Superflex $200 room → save as `data/fpros_auction.csv`.
   - FantasyPros projections or half-PPR overall rankings with projected points → save as CSV(s).
   - Yahoo ADP (overall) for 2025 half-PPR → save as `data/yahoo_adp.csv`.
   - (Optional) Add a `risk_score` column [0..1].
3) Edit the CONFIG cell as desired.
4) Run Build Market → Optimize → Scenarios.
5) Check `outputs/` for saved rosters.
