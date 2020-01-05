# Assignment 1 CS651
## Name: Harshpal Singh
## Student ID: 20752839
## Enrolled in CS631

### Q1
<p>Pairs PMI: This tasks in split in 2 map reduce jobs. The first job takes as input text file (long, Text is passed to mapper) and counts word frequency where a word's count is increased only once per every line and outputs it as (Text, IntWritable) key values to an intermediate file called "./countsFile/part-r-00000". The first job also counts total number of lines which is passed as a counter (custom enum counter LineCount) via context to the second job. The second job reads the original text file again and computes pair frequencies and also reads the intermediate file from job 1 to create in memory map of word frequencies. Job 2 final output is a (PairOfStrings, PairOfFloatInt) describing word pair as key and (PMI,count) as value to final ouput file. </p>
<p>Stripes PMI: It also has 2 mapreduce jobs where job 1 is same as PairPMI. Job 2 reads intermediate data and creates map of word frequencies same as job 2 of PairPMI. Job 2 creates a output record of Text, HashMapWritable<Text,PairOfFloatInt> as a stripe data record.</p>

### Q2
CS student linux env:
- Pairs with combiner: 23.176 seconds
- Stripes with combiner: 10.272 seconds

### Q3
CS student linux env:
- Pairs without combiner: 24.466 seconds
- Stripes without combiner: 11.223 seconds

### Q4
- 69098 distinct PMI pairs
- wc command result: 69098  276392 2079868

### Q5
Highest PMI:
1. (maine, anjou)	(3.6331422, 12)
2. (anjou, maine)	(3.6331422, 12)

Lowest PMI:
1. (thy, you)	(-1.5303967, 11)
2. (you, thy)	(-1.5303967, 11)

### Q6
'tears':
1. (tears, shed)	(2.1117902, 15)
2. (tears, salt)	(2.0528123, 11)
3. (tears, eyes)	(1.165167, 23)

'death':
1. (death, father's)	(1.120252, 21)
2. (death, die)	(0.75415933, 18)
3. (death, life)	(0.7381346, 31)

### Q7
1. (hockey, defenceman)	(2.4180872, 153)
2. (hockey, winger)	(2.3700917, 188)
3. (hockey, sledge)	(2.352185, 93)
4. (hockey, goaltender)	(2.2537384, 199)
5. (hockey, ice)	(2.2093477, 2160)

### Q8
1. (data, cooling)	(2.0979042, 74)
2. (data, encryption)	(2.0443726, 53)
3. (data, storage)	(1.9878386, 110)
4. (data, database)	(1.8893089, 99)
5. (data, disk)	(1.7835915, 68)
