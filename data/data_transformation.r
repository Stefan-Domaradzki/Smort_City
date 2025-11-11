library(dplyr)
library(tidyverse)
library(ggplot2)
library(stringr)

test.1 <- read.csv("./source/1. 15 - 21.03 2021.csv", 
                   header = TRUE, 
                   fileEncoding = "Windows-1250", 
                   encoding = "Windows-1250",
                   sep=";")

skrzyz <- unique(test.1$Skrzyżowanie)

summary(test.1)
ave

test.1[c(1:23),6]

mean(test.1[test.1$Relacja=="Wszystkie",6])
sd(test.1[test.1$Relacja=="Wszystkie",6])

mean(test.1[test.1$Skrzyżowanie=="al. Piłsudskiego - ul. Targowa",6])

dzien.t <- test.1[c(1:23),]

c(1:23)
nat <- dzien.t$Natężenie..poj.godz.


nat.og <- test.1[,c(2,4,5,6)]
nat.og[3] <- str_split_i(nat.og$Data.i.Czas," ",2)

nat.cien <- nat.og[nat.og$Skrzyżowanie==nat.og$Skrzyżowanie[1],]
?aggregate

as.list()

str(nat.cien$Data.i.Czas)
cien.agg <- aggregate(nat.cien$Natężenie..poj.godz.,
                      list(nat.cien$Data.i.Czas), 
                      FUN="mean")



ggplot(cien.agg, aes(x=cien.agg$Group.1, y=cien.agg$x)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(x = "x (1-23)", y = "NT", title = "Barplot zmiennej NT dla x=1:23") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))














