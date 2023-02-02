#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)
options(scipen=999)

library(doParallel,quietly=T)
library(DBI)
library(rjson)
library(stringr)

registerDoParallel(30)

# devel
web_root = "/dev/shm/dahlo/web/"

# get arguments if any
if(length(args) == 1){
    web_root = args[1]
}

# set variables
plot_web_dir    <- 'projects'
storage_systems <- c('crex')

# create directories
dir.create(paste(web_root,plot_web_dir,sep='/'), showWarnings=F, recursive=T)


# connect to the project_portal sqlite database
if( !('state' %in% ls())) {
    pp_db   <- dbConnect(RSQLite::SQLite(), '/sw/share/compstore/production/statistics/dbs/project_portal.sqlite')
    query   <- "SELECT * FROM current_state" 
    state   <- dbGetQuery(conn=pp_db, statement=query)
}

# loop to find all directory names
dirname_map <- data.frame(dirname = rep(NA, nrow(state)), 
                          proj_id = rep(NA, nrow(state)), 
                          start   = rep(NA, nrow(state)), 
                          end     = rep(NA, nrow(state))) 

for(i in 1:nrow(state))  {
    current_id <- strsplit(str_extract(state$data[[i]], 'Directory_Name\".*?,'), '\"')[[1]][3]
    dirname_map[i, 1:2] <- c(ifelse(current_id == '' | is.na(current_id),  state$proj_id[i], current_id), state$proj_id[i]) 

    start      <- strsplit(str_extract(state$data[[i]], 'Start\".*?,'), '\"')[[1]][3]
    end        <- strsplit(str_extract(state$data[[i]], 'End\".*?,'), '\"')[[1]][3]
    dirname_map[i, 3:4] <- c(start, end)
}


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

pretty <- function(X) {
    sub(perl=T,'^\\s+','',format(X,big.mark=' '))
}

plot_storage_timeline <- function( curstate, state, dirname_map, filename, storage_systems, width=1400, height=900, pointsize=26) {
    # Settings
    dev <- F
    mar = c(5.4,6.3,3,7)
    las = 1
    yaxs = 'i'
    xaxs = 'i'
    bluecol <- '#3399ccbf'
    redcol  <- '#cc6666bf'

    # Set project id as directory name if missing
    proj_id <- curstate$Projectname
    #dirname <- ifelse(curstate$Directory_Name == '', proj_id, curstate$Directory_Name)
    if( is.null(curstate$Directory_Name ) ) {
        dirname <- proj_id
    } else if ( curstate$Directory_Name == '') {
        dirname <- proj_id
    } else {
        dirname <- curstate$Directory_Name
    }


    # Init
    storage     <- data.frame(NA,NA,NA,NA,NA)[-1,]
    allocations <- data.frame(NA,NA,NA,NA)[-1,]

    #if( length(which(dirname_map$dirname == dirname)) < 2) return()
    print(proj_id)
    # Gather all usage information
    for(i in  which(dirname_map$dirname == dirname)) {
        storage_systems <- 'crex'
        curproj <- fromJSON(state$data[[i]]) 

        # Add project name if null
        if(is.null(curproj$Directory_Name )) {
            curproj$Directory_Name <- curproj$Projectname
        } else if ( curproj$Directory_Name == '') {
            curproj$Directory_Name <- curproj$Projectname
        } 

        # Skip if no storage data
        if(length(curproj$storage) == 0) next

        # Check if storage system is in curproj
        storage_index <- grep(paste0(storage_systems, collapse='|'),names(curproj$storage))


        if(length(storage_index) > 1) print('length storage_index > 1')
        data    <- curproj$storage[[storage_index]]
        data    <- data[!(names(data) %in% 'updated')]
        data    <- data[order(names(data))]

        # Select only storage allocations
        allo    <- curproj$Allocations
        allo    <- allo[unlist(lapply(allo, function(x) { x$Type})) %in% 'storage']

        # Get size and freq data to plot
        storage     <- rbind(storage,cbind(unlist(lapply(data,'[[',2)), 
                                           unlist(lapply(data,'[[',1)),
                                           as.Date(names(data)),
                                           curproj$Directory_Name,
                                           curproj$Projectname))

        allocations <- rbind(allocations, cbind(as.Date(unlist(lapply(allo,'[[','Start'))),
                                                as.Date(unlist(lapply(allo,'[[','End'))),
                                                unlist(lapply(allo,'[[','Value')) * 1024^3))  # SAMS reports in Gigabyte

    }
    # Set colnames
    colnames(storage)     <- c('usage', 'freq', 'date', 'dirname', 'proj_id')
    colnames(allocations) <- c('start', 'end', 'value')

    # Sort storage and remove duplicates if overlapping dates and convert value to numeric
    #if(any(duplicated(storage$date))) storage <- storage[storage$proj_id == proj_id,]
    if(sum(duplicated(storage$date)) > 1) storage <- storage[storage$proj_id == proj_id,]
    storage$usage <- as.numeric(storage$usage)
    storage$freq <- as.numeric(storage$freq)
    storage <- storage[order(storage$date, storage$proj_id),]

    # Set xlim and ylim
    xlim              <- c(Sys.Date() - 365, Sys.Date() - 1)
    ylim              <- c(0, max(storage$usage, allocations$value)) * 1.00

    # Plot empty background
    if(!dev) {
        png(filename, width=width, height=height,pointsize=pointsize,type='cairo')
    }
    par(mar=mar, las = las, yaxs = yaxs, xaxs = xaxs)
    plot(1, type='n', xlim=xlim, ylim=ylim, axes=F, ylab = '', xlab = '')

    # Set axes
    date_axis   <- seq.Date(from=as.Date('2000-01-01'), to= max(xlim) + 365, by='week')
    axis(1, date_axis, date_axis, las = 2)

    axis_seq <- seq(ylim[1], ylim[2], length.out=7)
    axis(2, at = axis_seq, 
         labels = gdata::humanReadable(axis_seq, digits = 1), 
         col.axis = bluecol,
         font = 2) 

    # Add mtext and title
    par(las=0)
    mtext('Data usage',      side = 2, line = 5.2)
    mtext('Number of files', side = 4, line = 0.8)
    mtext(proj_id,           side = 3, line = 0.3)

    title('Storage timeline', line = 1.7)
    par(las=las)

    # Plot allocation
    rect(xleft   = allocations$start, 
         xright  = allocations$end, 
         ybottom = 0, 
         ytop    = allocations$value,
         col     = redcol,
         lwd     = 0)

    # Plot usage
    rect(xleft   = as.Date(rownames(storage))[1:(nrow(storage) - 1)],
         xright  = as.Date(rownames(storage))[2:nrow(storage)],
         ybottom = 0,
         ytop    = storage$usage[2:nrow(storage)], 
         col     = bluecol, lwd=0)

    abline(h = axis_seq, lty=3, col = '#00000040')

    # Plot starts and ends for projects
    startends <- dirname_map[which(dirname_map$dirname == dirname),]
    text(x = as.Date(startends$start) + 5, y = ylim[2] / 2 , labels = paste0(startends$proj_id, ' start'), srt = 90)
    text(x = as.Date(startends$end)   - 5, y = ylim[2] / 2 , labels = paste0(startends$proj_id, ' end'  ), srt = 90)
    abline(v = as.Date(c(startends$start,startends$end)), lend=1, lwd = 1.2)


    # Plot freq
    # Plot empty background
    par(new=T, mar = mar, yaxs = yaxs, xaxs = xaxs)

    ylim              <- c(0, max(storage$freq)) * 1.1

    plot(1, type='n', xlim=xlim, ylim=ylim, axes=F, ylab = '', xlab = '')
    points(as.Date(rownames(storage)),storage$freq,type='l', col='black', lwd = 2)

    # Create y axis and custom rounding alghorithm
    axis_seq <- seq(ylim[1], ylim[2], length.out=7)
    axis_seq <- round( axis_seq, min( 0, -(str_length(round(axis_seq)) - 1 - 2)) )
    axis(4, at = axis_seq, labels = pretty(axis_seq), line=0, mgp = c(3,2,0)) 

    # Add legend
    legend(x = "topright", inset = c(0, -0.10), 
           legend = c("Usage", "Allocation", '#Files'), 
           pch = c(15,15,NA), lty=c(NA,NA,1), col = c(bluecol,redcol,'black'), 
           xpd = TRUE, horiz = TRUE, bty ='n', lwd=2, cex=0.9, pt.cex = 1.6, 
           text.width = c(26,24,10), x.intersp = 0.3, seg.len = .8) 
    box(bty='U')
    if(!dev) {
        dev.off()
    }

}

# pretty print numbers
pretty <- function(X) {
    sub(perl=T,'^\\s+','',format(X,big.mark=' '))
}


fix_suffix <- function(data, suffix = "auto") {
    #t(as.data.frame(strsplit(humanReadable(data, units = suffix, justify='none'), split=' ')))
    data.frame(matrix(unlist(strsplit(gdata::humanReadable(data, units = suffix, justify='none'), split=' ')),ncol=2, byrow=T))
}



# loop over projects
foreach(i = 1:nrow(state)) %dopar% {
#for(i in 1:nrow(state)) { 
    
    # set parameters 
    #i           <- which(state$proj_id == 'snic2021-6-267')
    #i           <- 4636
    proj_id     <- state$proj_id[i]
    curstate    <- fromJSON(state$data[i])
    
    # plot filesize
    if(length(curstate$filesize) > 0) {
        # create project dir in plot_web_dir
        dir.create(paste(web_root,plot_web_dir,proj_id, sep='/'),showWarnings=F)

        total_size      <- sum(data.frame(curstate$filesize$project$exts)[1,])
        total_human     <- fix_suffix(total_size)
        total_size      <- total_human[1]
        total_suffix    <- total_human[2]

        # 1st plot. Extension size
        data            <- sort(t(data.frame(curstate$filesize$project$exts))[,1], decreasing=T)

        # Remove dots in ext names
        names(data)     <- gsub('\\.','',names(data))
        
        # Create human readable suffixes
        suffix          <- fix_suffix(data, total_suffix)
        suffix[,1]      <- round(as.numeric(suffix[,1]),2)

        # If a project only contains empty files. Add 1 byte.
        data[data==0]   <- 1

        # plot only the 50 first extensions and skip if less than 2
        if(length(data) > 50) data <- data[1:50] 
        #if(length(data) < 1)  return() 
        
        filename        <- paste(web_root,plot_web_dir, proj_id, paste0(proj_id, '_ext_size.png'),sep='/')
        labels          <- paste0(names(data),', ', suffix[,1])
        title           <- paste0("Extension size , Tot: ", paste(total_human, collapse = ' '))
        plot_pie(data, title, labels, filename)




        # 2st plot. Extension freq
        data            <- sort(t(data.frame(curstate$filesize$project$exts))[,2], decreasing=T)
        # plot only the 50 first extensions and skip if less than 2
        if(length(data) > 50) data <- data[1:50] 

        filename        <- paste(web_root,plot_web_dir, proj_id, paste0(proj_id, '_ext_freq.png'),sep='/')
        labels          <- paste0(names(data),', ', pretty(data))
        title           <- paste0("Number of files by extension, tot: ", pretty(sum(data)))
        plot_pie(data, title, labels, filename)




        # 3rd plot. Size per year
        data            <- data.frame(curstate$filesize$project$years)[1,,drop=F]

        # Create human readable suffixes
        suffix          <- fix_suffix(data, total_suffix)
        suffix[,1]      <- round(as.numeric(suffix[,1]),2)

        # If a project only contains empty files. Add 1 byte.
        data[data==0]   <- 1

        filename        <- paste(web_root,plot_web_dir, proj_id, paste0(proj_id, '_year_size.png'),sep='/')
        labels          <- paste0(gsub('X', '', names(data)),', ', suffix[,1])
        title           <- paste0("File size by file date, tot: ", paste(total_human, collapse = ' ')) 
        sub             <- 'Based of file modification date'

        plot_pie(as.matrix(data), title, labels, filename)




        # 4th plot. Locations
        data            <- as.data.frame(lapply(curstate$filesize$project$locations,'[[',1))

        # Create human readable suffixes
        suffix          <- fix_suffix(data, total_suffix)
        suffix[,1]      <- round(as.numeric(suffix[,1]),2)

        # If a project only contains empty files. Add 1 byte.
        data[data==0]   <- 1

        names(data)     <- tools::toTitleCase(names(data))

        filename        <- paste(web_root, plot_web_dir, proj_id, paste0(proj_id, '_locations_size.png'), sep='/')
        labels          <- paste0(names(data),', ', suffix[,1])
        title           <- paste0( "Backup vs nobackup size, tot:  ", paste(total_human, collapse = ' '))
        plot_pie(as.matrix(data), title, labels, filename)



        # 5th plot. Storage timeline
        if(length(curstate$storage) != 0) {
            filename    <- paste(web_root, plot_web_dir, proj_id, paste0(proj_id, '_storage_timeline.png'), sep='/')
            plot_storage_timeline(curstate, state, dirname_map , filename, storage_systems)
        }
        

        # 6th plot. Core hour timeline
        #data            <- as.data.frame(lapply(curstate$filesize$project$locations,'[[',1))



        # Return 0
        a <- 0
    }
}


