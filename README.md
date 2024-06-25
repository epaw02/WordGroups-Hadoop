The aim of the project is to calculate statistics for groups of words which are equal up to permutations of letters. For
example, ‘emit’, ‘item’ and ‘time’ are the same words up to a permutation of letters.
With the use of Apache Hadoop the task is to determine such groups of words and sum all their counts. 

Output record: number of unique words in the group, count of occurrences for the group of
words, list of the words in the group in lexicographical order:
groupSize <tab> occurrences <tab> word1 word2 word3 ...

Example: assume ‘emit’ occurred 3 times, 'item' 2 times, 'time' 5 times; 3 + 2 + 5 = 10; group
contains 3 words, so for this group result is:
3 10 emit item time
