#count words
import glob

#map
map_res=[]
for file in glob.glob("/home/local/Downloads/pg*"):
    with open(file) as f:
        for line in f:
            line=line.strip().split()
            map_res.extend(list(map(lambda x: (x,1),line)))



#sort
map_res.sort(key=lambda x:x[0])    


#reduce
current_word=None
current_count=0
word=None
reduce_rel=[]
with open("/home/local/Downloads/pg.txt","w") as f:
    for item in map_res:
    #if len(item)>1:
        word=item[0]
        count=item[1]
    #else:
    #    continue

        if current_word==word:
            current_count+=count
        else:
            if current_word:
                #print(f"{current_word}\t{current_count}")
                #f.write(f"{current_word}\t{current_count}\n")
                reduce_rel.append((current_word,current_count))
            current_word=word
            current_count=count
    if current_word==word:
        #print(f"{current_word}\t{current_count}")
        #f.write(f"{current_word}\t{current_count}\n")
        reduce_rel.append((current_word,current_count))

reduce_rel.sort(key=lambda x:x[1])
for item in reduce_rel:
    print(item,end="\n")
