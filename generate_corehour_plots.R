#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

library(DBI)
library(hash)
library(dplyr)
# devel
root_dir = "/dev/shm/bjornv/out_zst_incomplete.2/"
web_root = "/dev/shm/dahlo/web/"

# init
clusters = c("rackham","snowy","sens-bianca")

# get arguments if any
if(length(args) == 2){
    root_dir = args[1]
    web_root = args[2]
}

# define variables
jobs = hash()
eff  = hash()
time_now <- as.integer(Sys.time())
time_30d <- time_now - 30*24*3600
date_31d <- as.Date(as.POSIXct(time_30d - 86400, origin="1970-01-01"))

# initiate sqlite databases
for(cluster in clusters){
    jobs[cluster] <- dbConnect(RSQLite::SQLite(), paste0("/sw/share/compstore/production/statistics/dbs/slurm_accounting/", cluster, ".sqlite"))
    eff[cluster]  <- dbConnect(RSQLite::SQLite(), paste0("/sw/share/compstore/production/statistics/dbs/efficiency/", cluster, ".sqlite"))
}
# set query
#query <-  paste0("SELECT job_id, user, start, end, cores FROM slurm_accounting WHERE start > ", time_30d, " AND proj_id LIKE '%' AND end LIKE '%' AND cluster LIKE '%'") 

# get project list
#projects <- list.files(paste0(root_dir, '/tmp/'))
#projects <- projects[grepl('years.csv', projects)]
#projects <- projects[projects != "all.years.csv"]
#projects <- sub('.years.csv','', projects)


# get all jobs for a project
for (cluster in clusters) {

    # read sql slurm_accounting 
    query            <- paste0("SELECT proj_id, job_id, user, start, end, cores FROM slurm_accounting WHERE end > ", time_30d) 
    slurm_accounting <- dbGetQuery(conn=values(jobs[cluster])[[1]], statement=query)

    # read sql efficiency 
    query            <-  paste0("SELECT job_id, cpu_mean, mem_peak, mem_limit FROM efficiency WHERE date_finished > '", date_31d, "'") 
    efficiency       <- dbGetQuery(conn=values(eff[cluster])[[1]], statement=query)

    # merge both databases
    job_stats        <- merge(slurm_accounting,efficiency,by='job_id', sort = T)
    
    # rescale cpu mean
    job_stats$cpu_mean   = job_stats$cpu_mean / 100

    # calculate core hours
    job_stats$corehours  = (job_stats$end - job_stats$start ) / 3600 * job_stats$cores

    # calculate efficiency
    job_stats$mem_eff    = job_stats$mem_peak / job_stats$mem_limit
    job_stats$efficiency = pmax( job_stats$cpu_mean , job_stats$mem_eff) 


    # calculate the efficiency volume
    job_stats$eff_vol = job_stats$corehours * job_stats$efficiency
    proj_stats        = job_stats %>% 
                        group_by(proj_id) %>% 
                        summarize(sum_corehours = sum(corehours),
                                  sum_eff_vol = sum(eff_vol),
                                  sum_cpu_vol = sum(corehours * cpu_mean),
                                  sum_mem_vol = sum(corehours * mem_eff)) %>% 
                        summarize(proj_id,
                                  corehours = sum_corehours,
                                  proj_eff = sum_eff_vol / sum_corehours,
                                  proj_cpu_eff = (sum_cpu_vol / sum_corehours),
                                  proj_mem_eff = (sum_mem_vol / sum_corehours))

    # process each project
    projects <- unique(job_stats$proj_id)
    for (project in projects) {

        print(project)
        # skip projects that have not run anything (perhaps we should plot empty graphs for these?)
        #if (!(project %in% job_stats$proj_id)) next()
        #jobs <- job_stats[job_stats$proj_id == project,]

        # create project folder if it does not exist yet
        dir.create(file.path(web_root, "projects", project), showWarnings = FALSE, recursive=T)

        png(paste0(web_root, "/projects/", project, "/", project, "_mean_efficiency.png"), width = 900, height = 900)
        par(las=1, cex=2)
        plot(proj_stats$proj_cpu_eff, proj_stats$proj_mem_eff, col='black', bg='#00000050', pch=21, cex=2, xlim=c(0,1), ylim=c(0,1), xlab="", ylab="")
        par(new=T)
        plot(proj_stats$proj_cpu_eff[proj_stats$proj_id == project], proj_stats$proj_mem_eff[proj_stats$proj_id == project], col='black', bg='red', pch=21, cex=5, xlim=c(0,1), ylim=c(0,1), axes=F, xlab="", ylab="")
    #    mtext('Other projects',3,outer=T,line=-2,0)
    #    mtext(project,3,outer=T,line=-2, adj=50)
        legend('topright', legend=c(project, "Other projects"), pch=21, col='black', pt.bg=c("red", "#00000050"), pt.cex=2)
        dev.off()
    }
}

# get efficiency for all jobs

# merge dataframes

# plot summary


