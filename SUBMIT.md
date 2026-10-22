# Submitting a TyO cup

After hosting a Tag You're Out cup, you can put it on the leaderboard yourself, straight from the Top->Out results file.

Page: https://aizpunr.github.io/TyO-Rankings/submit.html (also "Submit a cup" at the bottom of the leaderboard).

## 1. Find the file

The Top->Out mod saves the results on the host's PC:

```
%APPDATA%\Zeepkist\Mods\TopOutTournament\topoutresults_bckp.json
```

Paste that path into the Explorer address bar to open the folder.

Send it soon after the cup. The mod replaces this file when your next Top->Out event ends, so a cup that is not sent before then is lost.

## 2. Drop it on the page

The page reads the file in your browser. Nothing is uploaded until you press Submit. It shows the winner, the podium, the maps and the full results. Compare them with what you saw in game.

The cup number and the date are filled in for you: the number is the next one after the last cup on the site, and the date comes from when the file was saved. Change them only if they are wrong.

The page refuses two kinds of file:

- **The previous cup again.** If you send the file before the new cup has ended, it still holds the last cup.
- **An unfinished cup.** If more than one player is still alive after the last round, the cup did not finish (or this is the live file, `topoutresults.json`).

## 3. Send it

Complete the anti-bot check and press Submit. An automatic job checks for new cups every 15 minutes from Saturday evening to Sunday midday (Spain time). The "Recent submissions" table shows what happened:

| Status | Meaning |
|---|---|
| received | Waiting for the automatic job. |
| published | Live on the leaderboard. |
| processed | Something looked unusual (a skipped cup number, a date that is not a Saturday, an unusual format), so aizpun checks it before it goes live. The note says what. |
| duplicate | That cup is already on the leaderboard, or this exact file was already sent. Every file is kept and checked against the published result. |
| superseded | A newer file for the same cup was sent, and that one is used. |
| failed | It could not be processed. The note says why. Ping aizpun. |

If you send a corrected file for a cup that is still waiting, the newer file is used.

## Privacy

The results file holds the cup settings, the maps, and each player's Steam ID, in-game name, times and lives per round. The leaderboard already publishes the same data, and the file itself is stored with the site's cup logs. The service stores the name you type (if any). It does not store your IP address.
