library(readr) library(data.table)
FileName=paste0(Base,'/IP_DATA_ALL.csv') IP_DATA_ALL <- read_csv(FileName)
hist_country=data.table(Country=unique(IP_DATA_ALL$Country)) pattern_country=data.table(Country=hist_country$Country,PatternCountry=hist_country$Countr y)
oldchar=c(letters,LETTERS) newchar=replicate(length(oldchar),"A") for (r in seq(nrow(pattern_country))){ s=pattern_country[r,]$PatternCountry; for (c in seq(length(oldchar))){ s=chartr(oldchar[c],newchar[c],s)
};
for (n in seq(0,9,1)){ s=chartr(as.character(n),"N",s)
};
s=chartr(" ","b",s)
s=chartr(".","u",s) pattern_country[r,]$PatternCountry=s;
};
View(pattern_country)
-----------------------------------------------
library(readr) library(data.table)
FileName=paste0(Base,'/IP_DATA_ALL.csv') IP_DATA_ALL <- read_csv(FileName)
hist_latitude=data.table(Latitude=unique(IP_DATA_ALL$Latitude)) pattern_latitude=data.table(latitude=hist_latitude$Latitude, Patternlatitude=as.character(hist_latitude$Latitude)) oldchar=c(letters,LETTERS)  newchar=replicate(length(oldchar),"A")
for (r in seq(nrow(pattern_latitude))){ s=pattern_latitude[r,]$Patternlatitude; for (c in seq(length(oldchar))){
 
s=chartr(oldchar[c],newchar[c],s)
};
for (n in seq(0,9,1)){ s=chartr(as.character(n),"N",s)
};
s=chartr(" ","b",s)
s=chartr("+","u",s)
s=chartr("-","u",s)
s=chartr(".","u",s) pattern_latitude[r,]$Patternlatitude=s;
};
setorder(pattern_latitude,latitude) View(pattern_latitude[1:3])

