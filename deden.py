import os
import sys

class DOC_LINE:
	def docline(self, file):
		count = 0
		w = open('doc_len.txt', 'w')
		with open(file) as f:
			for line in f :
				
				w2d = line.replace("\n", "").strip()
				#print w2d
				if w2d == "":
					#print w2d
					continue
				w2d = w2d.split()
				
				if w2d[0] == "<DOCNO>" :
				#	print "count = %d"%(count)
					w.write(":%d\n"%(count))
					count = 0
				#	print w2d[1]
					w.write(w2d[1])
				else :
					count += len(w2d)

		#print "count = %d"%(count)
		w.write(":%d"%(count))
if __name__ == "__main__":
	doc = DOC_LINE()
	doc.docline("seded_AP88.txt")
