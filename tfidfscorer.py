import math
import stemmer
import sqlite3

class TF_IDF:
	def __init__(self):
		
		self.porter = stemmer.Stemmer()
		
	def DB_build(self, post_list_path):
		self.param = {"idf_N":0, "icf_N":0,}
		self.conn = sqlite3.connect("./tfidf_full.db")
		cur = self.conn.cursor()
		
		
		def file_to_dict(file_path):
			temp_post_list = {} # term->{'docs':{doc_name:term_freq}, 'doc_freq':int}
			col_N = 0
			doc_N = 0
			with open(file_path) as f:
				for line in f:
					w2d = line.replace("\n", "").strip().split(":")
					term, docs = w2d[0].strip().split(), w2d[1].strip().split()
					col_freq = int(term[1].replace("[", "").replace("]", ""))
					col_N += col_freq
					#print("col_N = %d"%(col_N))
					temp_post_list[term[0]] = {"col_freq":col_freq, "docs":{}} #
					for doc in docs:
						fnp = doc.split("#")
						f_path, term_freq = fnp[0], int(fnp[1])
						temp_post_list[term[0]]["docs"][f_path] = term_freq
					doc_freq = len(temp_post_list[term[0]]["docs"].items())
					temp_post_list[term[0]]["doc_freq"] = doc_freq
					doc_N += doc_freq
			return temp_post_list, col_N, doc_N
            
		self.posting_list, self.param["icf_N"], self.param["idf_N"] = file_to_dict(post_list_path)
        
		cur.execute("insert into meta(idf, icf) values(?, ?)", (self.param["idf_N"], self.param["icf_N"]))

		term_adder = "insert into terms(term, doc_freq, col_freq) values(?, ?, ?)"
		doc_adder = "insert into docs(term, doc_id, freq) values(?, ?, ?)"
		
		for term in self.posting_list:
			docs = self.posting_list[term]["docs"]
			doc_freq = self.posting_list[term]["doc_freq"]
			col_freq = self.posting_list[term]["col_freq"]
			cur.execute(term_adder, (term, doc_freq, col_freq))
			for doc_id in docs:
				cur.execute(doc_adder, (term, doc_id, docs[doc_id]))
		self.conn.commit();
		self.conn.close();
		
	def word_tf(self, term_freq, doc_avg_len, doc_now_len, term_query_freq):
		k1 = 1.3
		b = 0.75
		k2 = 1.7
		#return 1+math.log(term_freq*1.0)
		return (term_freq * (k1 + 1) / (term_freq + k1 * ((1 - b) + b * (doc_now_len / doc_avg_len)))) * ((k2 + 1) * term_query_freq / (k2 + term_query_freq))
		
	def word_idf(self, idf, term_doc_freq):
		return math.log(idf*1.0/term_doc_freq)
		
	def make_dict_len(self, file_path):
		temp_doc_len = {} #{'doc_id' : string, length : int}
		print "making  len dictionary ..."
		with open(file_path) as f:
				for line in f:
					w2d = line.replace("\n", "").strip().split(":")
					doc_id = w2d[0]
					length = int(w2d[1])
					temp_doc_len[doc_id] = length
		print "...end"		
		return temp_doc_len
		
	
	def calc_sent_tfidf(self, sentence, doc_len_dict):
		self.conn = sqlite3.connect("./tfidf_full.db")
		cur = self.conn.cursor()
		cur.execute("select * from meta")
		#idf_N = cur.fetchall()[0][0]
		#idf_N = 79928
		#doc_avg_len = 35937138 / 79928
		#doc_avg_len = 36033984 / 79928
		idf_N = 79888.0
		doc_avg_len = 35937138.0 / 79888.0
		#doc_len_dict = self.make_dict_len("doc_len.txt")
		#print ("idf_N = %d"%(idf_N))
		
		doc_finder = "select * from docs where term=?"
		term_finder = "select * from terms where term=?"
		doc_lenth_full = "select sum(freq) from docs"   
		#cur.execute()
		doc_len = "select sum(freq) from docs where doc_id = ?" 
		
		
		
		
		#for doc_id in doc:
		#	doc_now_len[] = 
		
		query = sentence.strip().split()
		#print ("query_ken = %d"%(len(query)))
		score_lst = {} # doc_id -> score
		for term in query:
			
			term_query_freq = 0.0
		
			for terms in query :
				if terms == term :
					term_query_freq += 1.0
			
			cur.execute(doc_finder, (term,))
			docs = cur.fetchall()
			#print ("len docs = %d"%(len(docs)))
			if len(docs) == 0:
				continue
			cur.execute(term_finder, (term,))
			term_doc_freq = cur.fetchall()[0][1]
			#i = 1
			for doc in docs:
				doc_id = doc[1]
				#print "[%7d] doc_id = %s "%(i, doc_id)
				term_freq = doc[2]
				doc_now_len = doc_len_dict[doc_id]
				
				if doc_id in score_lst:
					score_lst[doc_id] += self.word_tf(term_freq, doc_avg_len, doc_now_len, term_query_freq)*self.word_idf(idf_N, term_doc_freq)
				else:
					score_lst[doc_id] = self.word_tf(term_freq, doc_avg_len, doc_now_len, term_query_freq)*self.word_idf(idf_N, term_doc_freq)
		self.conn.close();
		return score_lst
        
	def print_sorted_tfidf(self, sent):
		for word in self.porter.remove_symbol(sent.lower()).replace("\n","").split():
			sentence.append(self.porter.stem(word, 0, len(word)-1))
		sentence = " ".join(sentence)
		print sentence
		sc_lst = self.calc_sent_tfidf(sentence, doc_len_dict)
		sc_lst = sorted(sc_lst.items(), key=(lambda x:x[1]), reverse=True)
        
		print "stemmed input query: %s"%sentence
		print " [doc_no | tf-idf]"
		for doc, score in sc_lst[:5]:
			print " [%s | %f]"%(doc, score)
		print "="*50
        
	def save_sorted_tfidf(self, save_path, query_file):
		save = open(save_path, 'w')
		start = 202
		doc_len_dict = self.make_dict_len("doc_len.txt")
		querys = open(query_file)
		for query in querys:
			temp_query = []
			temp = self.porter.remove_symbol(query.lower()).replace("\n", "").replace("\r", "").split()
			for word in temp:
				temp_query.append(self.porter.stem(word, 0, len(word)-1))
			temp_query = " ".join(temp_query)
			sc_lst = self.calc_sent_tfidf(temp_query, doc_len_dict)
			sc_lst = sorted(sc_lst.items(), key=(lambda x:x[1]), reverse=True)
			
			for i, (doc, score) in enumerate(sc_lst[:1000]):
				save.write("%d Q%d %s %d %f %s\n"%(start, start-1, doc, i+1, score, "myname"))
			start += 1
		save.close()
		querys.close();
			

if __name__ == "__main__":
	
	scorer = TF_IDF()
	#scorer.DB_build("./awked_AP88s.txt")
	#scorer.make_dict_len()
	scorer.save_sorted_tfidf("AP88_result.txt", "topics.202-250.txt")
	#print "="*50
	#while True:
	#	query = raw_input("input query: ")
	#	scorer.print_sorted_tfidf(query)
    
