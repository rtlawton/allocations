if(!require(tidyverse)) install.packages("tidyverse", repos = "http://cran.us.r-project.org")
if(!require(caret)) install.packages("caret", repos = "http://cran.us.r-project.org")
if(!require(RMySQL)) install.packages("RMySQL", repos = "http://cran.us.r-project.org")
if(!require(corrr)) install.packages("corrr", repos = "http://cran.us.r-project.org")
if(!require(kableExtra)) install.packages("kableExtra", repos = "http://cran.us.r-project.org")
#
# Create database connection
#
pwd <- 'Insert password'
mydb <- dbConnect(MySQL(), user='root', password=pwd, dbname='prosper', host='localhost')
#
#Retrieve transactions pertaining to particular institution
#Select transactions after point at which statements were first imported
#Discard transactions with internal administrative function or involving cash
#
SQL <- "select journal.id as id, dt, ac, ac_amount, comm, protocol from journal inner join funds on journal.fu = funds.id WHERE (protocol = 'BA' OR protocol= 'Bidvest') AND (ac < 40 OR ac > 47) AND journal.id > 3471"
rs <- dbSendQuery(mydb, SQL)
qdata <- dbFetch(rs, n=-1)
SQL <- "select id, name from accounts where archived = 0"
rs <- dbSendQuery(mydb, SQL)
acs <- dbFetch(rs, n=-1)
acs <- acs %>%mutate(id = as.character(id))
acs <- acs %>%mutate(id = as.factor(id))
qdata <- qdata %>% mutate(ac = factor(ac, levels = levels(acs$id)))
acs <- acs %>% inner_join(qdata %>% group_by(ac) %>% summarise(c=n()) %>% rename(id=ac),
                          c("id")) %>% select(-c)
#
# Transform ID field as integer, and extract day from date field
#
qdata <- qdata %>% mutate(id = as.integer(id), ac = as.factor(ac))
qdata <- qdata %>% mutate(day=strtoi(substr(dt,9,10),10))
qdata <- qdata %>% select(-dt)
#
# Data exploration
#
qnrec <- qdata%>%group_by(protocol)%>%summarize(c=n())

freq_plot <- qdata %>% ggplot(aes(x=ac))  +
  geom_bar(position=position_dodge(), aes(fill=protocol)) +
  xlab("Allocation")
#remove ac with less than 10 instances
qdata <- qdata %>% group_by(ac) %>% filter(n() >=10) %>% ungroup()
am_plot <- qdata %>%
  ggplot(aes(x=ac, y=ac_amount)) + geom_boxplot() + xlab("Allocation") + ylab("Amount")
dy_tot <- qdata %>% group_by(ac) %>% summarize(ac_tot = sum(ac_amount))
dy_gp <- qdata %>% inner_join(dy_tot, by=c("ac")) %>% mutate(am_frac=ac_amount/ac_tot) %>% group_by(day,ac) %>%
  summarize(am_d_frac=sum(am_frac))
dy_plot <- dy_gp %>% ggplot(aes(x=ac, y=day)) + geom_tile(aes(fill=am_d_frac), colour = "white") + 
  scale_fill_gradient(low = "white",high = "steelblue", name="Proportion") +
  xlab("Allocation") + ylab("Day of month")

# Get final models

SQL <- "select * from Model_BA"
rs <- dbSendQuery(mydb, SQL)
BAmodel <- dbFetch(rs, n=-1)
SQL <- "select * from Model_Bidvest"
rs <- dbSendQuery(mydb, SQL)
Bidvestmodel <- dbFetch(rs, n=-1)



