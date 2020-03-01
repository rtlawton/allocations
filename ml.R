if(!require(tidyverse)) install.packages("tidyverse", repos = "http://cran.us.r-project.org")
if(!require(caret)) install.packages("caret", repos = "http://cran.us.r-project.org")
if(!require(RMySQL)) install.packages("RMySQL", repos = "http://cran.us.r-project.org")
if(!require(corrr)) install.packages("corrr", repos = "http://cran.us.r-project.org")
#
# Create database connection
#
pwd <- 'Insert password here'
mydb <- dbConnect(MySQL(), user='root', password=pwd, dbname='prosper', host='localhost')
#
#verify institution
#
args <- commandArgs(trailingOnly = T)
if (length(args) == 1) {
  protocol <- args[1]
  rs <- dbSendQuery(mydb, "SHOW TABLES LIKE 'Model_%'")
  plist <- dbFetch(rs, n=-1)
  if (!(paste("Model_", protocol, sep="") %in% plist[,1])) {
    stop("No machine learning for this protocol", call.=F)
  }
} else {
  stop("Illegal call", call.=F)
}

#Retrieve transactions pertaining to particular institution
#Select transactions after point at which statements were first imported
#Discard transactions with internal administrative function or involving cash
#
SQL <- paste("select journal.id as id, dt, ac, ac_amount, comm from journal inner join funds on journal.fu = funds.id WHERE protocol = '", protocol, "' AND (ac < 40 OR ac > 42) AND journal.id > 3471", sep="")
rs <- dbSendQuery(mydb, SQL)
qdata <- dbFetch(rs, n=-1)
#
# Transform ID field as integer, and extract day from date field
#
qdata <- qdata %>% mutate(id = as.integer(id), ac = as.factor(ac))
qdata <- qdata %>% mutate(day=strtoi(substr(dt,9,10),10))
qdata <- qdata %>% select(-dt)
#
# Start training
#
set.seed(200)
testindex <- createDataPartition(qdata$ac,1,0.2,list=F)
test_set <- qdata[testindex,]
train_set <- qdata[-testindex,]
#
# Remove allocations with less than 10 instances
#
train_set <- train_set %>% group_by(ac) %>% filter(n() >=10) %>% ungroup()
#
# We are going to split up the comments column by some delimiters, into text fragments
# Make space for 10 fragment columns
#
delim <- "#|-|\\ +|\\*" 
train_set <- train_set %>% separate(
  col = comm,
  into = c("w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10"),
  sep = delim,
  remove = T,
  extra = "drop",
  fill = "right"
)
#
# Set parameters for reducing number of fragments:
#
r_f_l <- 3 #Rare fragment limit - we will discard fragments with low occurence
n_d_l <- 3 #Non-discriminating limit - we will discard fragments that are common to multiple allocations
m_f_l <- 2 #Minimum length of fragment
train_set <- train_set %>% gather(
  key = "word_pos", 
  value = "frag", -ac,-id, -ac_amount, -day) %>%
  filter(!is.na(frag) & nchar(frag) >= m_f_l) %>%
  select(-word_pos)
train_set <- train_set %>% group_by(frag)
train_set <- train_set %>% filter(n() > r_f_l)
train_set <- train_set %>% filter(n_distinct(ac) < n_d_l) %>% ungroup()
#
# Remove duplicates (where same fragment occurs twice in one transaction)
#
train_set <- distinct(train_set)
#
#create a dictionary of fragments
#
dict <- train_set %>%
  select(frag, ac) %>%
  group_by(frag) %>%
  summarize(c = n()) %>%
  rowid_to_column('label') %>%
  mutate(label = paste("f", label, sep=""))
#
#Add column names back into train_set_g
#
train_set <- train_set %>% left_join(dict)
#
# Spread to wide matrix form, with a column for each fragment : the matrix "value" 
# will be "nval" set at 1 for all existing transaction-fragment permutations,
# and filled with zeros for non-existing permutations.
#
train_set <- train_set %>% mutate(nval = as.integer(1)) %>% select(-frag, -c)
train_set <- train_set %>% distinct() %>%
  pivot_wider(names_from = label, values_from = nval, values_fill=list(nval=0))
#
# Identify highly correlated fragment columns and remove one of pair
#
cor2 <- train_set %>% select(-id, -ac, -ac_amount, -day) %>%
  correlate() %>% shave() %>% stretch(na.rm = TRUE) %>%
  arrange(desc(r))
dup <- cor2 %>% filter(abs(r) > 0.9)
train_set <- train_set %>% select(-one_of(dup$y))
#
#reset output factors
#
train_set <- train_set %>% mutate(ac = as.factor(as.character(ac)))
x <- train_set %>% select(-ac, -id)
#
# We are going to build a simple tree for each allocation outcome, and then combine
# trees into one composite dataframe with the following structure:
#   line = index number of split
#   field = name of trasaction field in the split
#   split = value at split
#   left = index number of next split if actual value is less than split
#   right = index number of next split if actual value is greater than split
#   frag = actual fragment text referenced in split (for a frag column)
#   value = output value (if left=0 or right=0) and stop
#
# First prepare list of allocations to build trees for
#
ac_list <- train_set %>% group_by(ac) %>% summarize(c=n()) %>% .$ac
ac_list <- as.character(ac_list)
#
# Need a number of helper funtions to translate output from rpart into this dataframe
#
# 1.) Function to retrieve the line number of child (1,2,3...) from rpart.model$frame system (1,2,4,8...)
# nod=model$frame node number
# nct=model$splits ncat (-1 for < split and +1 for > split)
# dr=1 to get left child, -1 for right child
# nxt=row number beyond table where there is no child
# nlist=vector of nodes in model$frame
# llist=vector of lines in current tree dataframe
# vlist=vector of leaves in model$frame
#
child <- function(nod, nct, dr, nxt, nlist, llist, vlist){
  nd <- 2*nod + (1 + dr*nct)/2
  ifelse(any(nlist == nd),llist[which(nlist == nd)],
         ifelse(any(vlist == nd), 0, nxt))
}
#
# 2.) get the fragment string from column name
#
get_frag <- function(field){
  ifelse(any(dict$label == field), dict$frag[which(dict$label == field)],"")
}
#
# 3.) Remove recursively redundant lines in the final dataframe
#
doprune <- function(nodes){
  nxt <- nrow(nodes) + 1
  r <- which(nodes$left==nxt & nodes$right==nxt)
  if (length(r) > 0) {
    deadline <- nodes$line[r[1]]
    nodes <- nodes %>% filter(line != deadline)
    nodes <- nodes %>% mutate(
      left=ifelse(left==deadline, nxt-1,ifelse(left>deadline,left-1,left)),
      right=ifelse(right==deadline, nxt-1,ifelse(right>deadline,right-1,right)),
      line=ifelse(line>deadline,line-1,line))
    nodes <- doprune(nodes)
  }
  nodes
}
#
# 4.) Main function to convert rpart.model$frame and rpart.model$splits into dataframe
#
# For this function we need to set cut-off for acceptable probability of an allocation
alloc_cutoff <- 0.7
#
get_treetable <- function(pr, ac) {
  sr <- as.data.frame(pr$splits)
  rn <- rownames(pr$splits)
  # select only primary splits
  sr <- sr %>% mutate(field = rn) %>%
    filter(count > 0) %>% group_by(count) %>%
    summarize(field=first(field), ncat=first(ncat), index=first(index)) %>% 
    arrange(desc(count)) %>% rename(n = count)
  # Insert node indices into frame
  fr <- pr$frame %>% select(var, n, yval)
  nodelist <- as.integer(rownames(fr))
  fr <- fr %>% mutate(node = nodelist)
  # split off leaves from the frame
  leaf_ind <- which(fr$var=="<leaf>")
  leaves <- fr[leaf_ind,]
  nodes <- fr[-leaf_ind,]
  # remove leaves that don't meet the cut off for allocation
  leaves <- leaves %>% filter(yval >= alloc_cutoff)
  # add line index
  nr <- nrow(nodes)
  nodes <- nodes %>% mutate(line = seq(1:nr))
  # join with splits
  nodes <- inner_join(nodes, sr, c("n"))
  nodevector <- nodes$node
  linevector <- nodes$line
  leafvector <- leaves$node
  # Find the line reference for child nodes
  nodes <- nodes %>% 
    select(-var, -n, -yval) %>% 
    rename(split = index) %>% 
    mutate(left = mapply(child, node, ncat, SIMPLIFY = T,
                         MoreArgs=list(dr=1, nxt=nr+1, nlist=nodevector, llist=linevector,vlist=leafvector)),
           right = mapply(child, node, ncat, SIMPLIFY = T,
                          MoreArgs=list(dr=-1, nxt=nr+1, nlist=nodevector, llist=linevector,vlist=leafvector))) %>% 
    select(-node, -ncat) %>%
    mutate(frag = unname(sapply(field,get_frag)),
           value = ifelse(left*right==0,ac,""))
  # Remove redundant lines
  doprune(nodes)
}

# 5.) Basic function to train rpart model
#
# First define an empty dataframe for root only trees:
#
empty_tree <- data.frame(line=integer(), 
                         field=character(),
                         split=numeric(),
                         left=numeric(),
                         right=numeric(),
                         frag=character(),
                         value=character(),
                         stringsAsFactors=FALSE)
#
#
dotree <- function(ac) {
  set.seed(207)
  y <- ifelse(train_set$ac == ac,1,0)
  rpt <- train(x,y,method = "rpart",
               tuneGrid = data.frame(cp = seq(0.0, 0.05, len = 10)),
               minsplit = 10)
  pr = rpt$finalModel
  #root only - no tree - return empty data frame
  if (nrow(pr$frame) == 1) return(empty_tree)
  #Translate into dataframe format
  get_treetable(pr, ac)
}
#
# 6.) Function to combine trees into single dataframe
# acc_tree is existing composite tree
# new_tree is freshly generated  tree for allocation ac
#
add_tree <- function(acc_tree, ac) {
  new_tree <- dotree(ac)
  shift <- nrow(acc_tree)
  new_tree <- new_tree %>% mutate(line = line + shift, 
                                  left = ifelse(left==0, 0, left + shift),
                                  right = ifelse(right==0, 0, right + shift))
  acc_tree <- rbind(acc_tree, new_tree)
}
#
# FINALLY we can build the composite tree
#
final_tree <- reduce(ac_list, add_tree, .init=empty_tree)
#
# Copy model back into database
#
dbWriteTable(mydb, value = final_tree, name = paste("Model_",protocol, sep=""), append = F, overwrite = T)
#
#Get predictions from test set
#
test_pred <- function(ac_amount, comm, day) {
  frags <- strsplit(comm, delim)[[1]]
  i <- which(final_tree$line == 1)
  while (length(i > 0)) {
    if (final_tree[i,]$frag != "") {
      meet <- final_tree[i,]$frag %in% frags
    } else {
      meet <- get(final_tree[i,]$field) > final_tree[i,]$split
    }
    if (meet) {
      if (final_tree[i,]$right==0) {
        return (final_tree[i,]$value)
      } else {
        i <- which(final_tree$line == final_tree[i,]$right)
      }
    } else {
      if (final_tree[i,]$left==0) {
        return (final_tree[i,]$value)
      } else {
        i <- which(final_tree$line == final_tree[i,]$left)
      }
    }
  }
  return ("0")
}
pred <- mapply(test_pred, test_set$ac_amount, test_set$comm, test_set$day)
#
# Calculate performance parameters
#
prop_correct <- mean(pred == test_set$ac)
prop_unguessed <- mean(pred == "0")
prop_wrong <- mean(pred != "0" & pred != test_set$ac)
sql <- paste("UPDATE TestResults set correct=", prop_correct, 
             ", wrong=", prop_wrong,
             ", unguessed=", prop_unguessed,
             ", updated='", format(Sys.Date(), "%Y-%m-%d"),
             "' WHERE protocol= '" , protocol, "'",
             sep="")
stmt<-dbSendQuery(mydb,sql)
stop("Success!")
