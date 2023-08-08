# Magic the Gathering's standard format analysis

<p align="center">
  <img src="https://www.baixesoft.com/wp-content/uploads/2012/02/Magic-The-Gathering-Online-banner-baixesoft.jpg" />
</p>

[Magic: The Gathering](https://magic.wizards.com/en/intro) is a trading card game released in 1993 that to this day remains the most famous of the genre. Playing Magic became much more accessible after the release of its free-to-play digital adaptation _Magic: The Gathering Arena_, resulting in an abrupt increase in the number of games played since its launch in 2018.

Players interested in seeing their progress in the game choose to opt for third-party deck trackers. The most used deck tracker is provided by [Untapped](https://mtga.untapped.gg/). Having collected information from millions of games, this service provides a player with valuable information:
- Winrate statistics for decks,
- Track personal card collection,
- Competitive progress in MTGA's ladder system,
- In-game played card tracker.

### Summary

Data from the Untapped website was scrapped daily, normalized and persisted on a **MySQL** database. The data is then used to build a **Metabase** dashboard intended to address questions listed in the following section. The scrapper scripts, dashboard and database are running on the **AWS** cloud, using a combination of the **EC2** and **RDS** services.

### Dashboard

![Dashboard](<images/MTGA - Dashboard.png>)

You can find the dashboard at [mtga.brunorateiro.com](http://mtga.brunorateiro.com/)

### Questions Answered

The questions regarding card competitiveness and popularity are listed below.
Clicking on each question will direct you to the corresponding modified SQL query.

- [Which cards are most popular and have the highest win rates in the current meta?](queries/latest_website_analytics.sql)
- [How are popularity and win rate associated across multiple color combinations](queries/win_rate_vs_popularity_by_color_identity.sql)
- [Which color combination has the highest overall win rate?](queries/win_rate_by_color_identity.sql)
- [Which color combination is the most popular?](queries/popularity_by_color_identity.sql)
- [Do certain types of cards (e.g., creatures, spells, enchantments) tend to have higher win rates?](queries/win_rate_by_type.sql)
- [Do legendary cards tend to have higher win rates?](queries/win_rate_vs_legendary_status.sql)
- [Which mana costs (converted mana costs) tend to correlate with higher win rates?](queries/win_rate_vs_cmc.sql)
- [Do cards with higher rarity (e.g., rare, mythic rare) generally have higher win rates compared to cards with lower rarity (e.g., common, uncommon)?](queries/win_rate_vs_rarity.sql)
- [How does the win rate and popularity of a card change over time?](queries/win_rate_and_popularity_overtime.sql)

### Entity relationship diagram

The following diagram was created using [Lucidchart](https://www.lucidchart.com/).

![Alt text](<images/Untapped's ER diagram.png>)

### Technologies and tools

<p align="center">
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/docker/docker-plain-wordmark.svg" width="60" height="60"/>
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/pandas/pandas-original-wordmark.svg" width="60" height="60"/> <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/sqlalchemy/sqlalchemy-original.svg" width="60" height="60"/> <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/numpy/numpy-original-wordmark.svg" width="60" height="60"/> <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original-wordmark.svg" width="60" height="60"/> <img src="https://www.cdnlogo.com/logos/m/19/metabase.svg" width="60" height="60"/> <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/amazonwebservices/amazonwebservices-original-wordmark.svg" width="60" height="60"/>
</p>

### Progress

The project is deemed as completed. If you have any suggestions or feedback, please don't hesitate to contact me at [bruno.rateiro@gmail.com](mailto:bruno.rateiro@gmail.com).

### Acknowledgment
Special thanks to my mentor and friend [Elias Soares](https://github.com/eliassoares). Check out his work.