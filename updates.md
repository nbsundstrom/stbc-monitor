# Updates to make to the Grafana page

---
## Top row
- remove the memory utilization panel and widen the other boxes equally

---
## Resource utilization per job
- I want this plot to be a resource usilization plot, so we basically check the total resources in
the pool and have a histogram with bands that represent the proportion of available resources used
by each user so if its at 92% then we sum everybodies usage with the largest usage being summed first
making a large band at the bottom and then add up until we're at 92% usage with coloured filled bands
showing the percentage used by each person

---
## resource usage per job

I like the table panel but the plot below idk it doesnt feel great to me, it doesnt show any new information.
there must be a way to get the actual GPU usage right now so like how much v ram is actually being used
and how much ram and cpu usage is actually being used. I have seen other people have it but I dont know how to get it 
Please figure this out for me. because like yeah I am allocated 46gb of ram but I'm really only using 25 or something

---
## Job runtime leaderboard

this should be a per user on the cluster thing maybe put under the resource utilization per job so we can always
see whos hogging shit

## Plot on current job 
