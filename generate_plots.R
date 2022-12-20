#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)
options(scipen=999)

library(DBI)
library(rjson)

# devel
root_dir = "/dev/shm/dahlo/out_zst_incomplete.2/"
web_root = "/dev/shm/dahlo/web/"

# get arguments if any
if(length(args) == 2){
    root_dir = args[1]
    web_root = args[2]
}

# set variables
plot_web_dir <- 'plots_web'

# create directories
dir.create(paste(root_dir,plot_web_dir,sep='/'),showWarnings=F)


# connect to the project_portal sqlite database
pp_db   <- dbConnect(RSQLite::SQLite(), '/sw/share/compstore/production/statistics/dbs/project_portal.sqlite')
query   <- "SELECT * FROM current_state" 
state   <- dbGetQuery(conn=pp_db, statement=query)



# define functions
# plot Web
plot_pie <- function(data, title, labels, filename, sub="", title_line=-1, width=1400, height=900, pointsize=26, mar=c(0,8,1,8)) {
    png(filename, width=width, height=height,pointsize=pointsize,type='cairo')
    par(mar=mar)
    pie(data,
        clockwise=T, 
        labels=labels,
        init.angle=0,
        lty=0,
        col= RColorBrewer::brewer.pal(name='Set1',9)[1:min(ncol(data),9)])
    title(main=title, sub=sub, line=title_line,cex.main=2)

    dev.off()

}

# pretty print numbers
pretty <- function(X) {
    sub(perl=T,'^\\s+','',format(X,big.mark=' '))
}



# loop over projects
#foreach(i = 1:nrow(state) %dopar% {
for(i in 1:nrow(state)) { 
    
    # set parameters 
    i           <- 7442
    i           <- which(state$proj_id == 'snic2020-2-19')
    proj_id     <-          state$proj_id[i]
    curstate    <- fromJSON(state$data[i])
    
    # plot filesize
    if(length(curstate$filesize) > 0) {
        # create project dir in plot_web_dir
        dir.create(paste(root_dir,plot_web_dir,proj_id, sep='/'),showWarnings=F)

        # 1st plot. Extension size
        data        <- sort(t(data.frame(curstate$filesize$project$exts))[,1], decreasing=T)
        data_GB     <- round(data / 1024^3,1)
        data_GB_sum <- round(sum(data) / 1024^3,1)
        # plot only the 50 first extensions and skip if less than 2
        if(length(data) > 50) data <- data[1:50] 
        if(length(data) < 1)  return() 

        filename    <- paste(root_dir,plot_web_dir, proj_id, paste0(proj_id, '_ext_size.png'),sep='/')
        labels      <- paste0(names(data),', ', pretty(data_GB))
        title       <- paste0("Extension size , Tot: ",pretty(data_GB_sum), ' GB')
        plot_pie(data, title, labels, filename)


        # 2st plot. Extension size
        data        <- sort(t(data.frame(curstate$filesize$project$exts))[,2], decreasing=T)
        # plot only the 50 first extensions and skip if less than 2
        if(length(data) > 50) data <- data[1:50] 

        filename    <- paste(root_dir,plot_web_dir, proj_id, paste0(proj_id, '_ext_freq.png'),sep='/')
        labels      <- paste0(names(data),', ', pretty(data))
        title       <- paste0("Number of files by extension ",pretty(data_GB_sum), ' GB')
        plot_pie(data, title, labels, filename)


        # 3rd plot. Size per year
        data            <- data.frame(curstate$filesize$project$years)[1,]
        names(data)  <- sub('X', '', names(data))
        data_GB         <- round(data / 1024^3,1)

        filename        <- paste(root_dir,plot_web_dir, proj_id, paste0(proj_id, '_year_size.png'),sep='/')
        labels          <- paste0(names(data),', ', pretty(data_GB))
        title           <- "File size by file date (GB)" 
        sub             <- 'Based of file modification date'
        browser()
        plot_pie(data, title, labels, filename)


        pieplotten har bara en färg. FIXA











    }


}
