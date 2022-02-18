
from typing import List
import re
from janome.tokenizer import Tokenizer

tokenizer = Tokenizer()

def tokenize(s: str) -> List[str]:
    words = []
    tokens = tokenizer.tokenize(re.sub(r'https://t.co/[0-9a-zA-Z]+', '', s))
    for token in tokens:
        surface = token.surface
        part = token.part_of_speech
        baseform = token.base_form
        #print(f'surface={surface}, part={part}, baseform={baseform}')        
        
        if(len(surface) > 32):
            # skip too long keyword 
            #print(f'[{surface}] dropped (too long)')
            pass
        elif(not re.match(r'^[0-9a-zA-Zぁ-んーァ-ヶｱ-ﾝﾞﾟ一-龠]+$', surface)):
            # skip non Japanese 
            #print(f'[{surface}] dropped (not Japanese)')
            pass
        elif(re.match(r'^[0-9a-zA-Zぁ-んァ-ヶｱ-ﾝﾞﾟのにはがとてー-十年月日時分秒火水木金土様方\\*]$', surface)
                    and not re.match(r'^[亀長黒戸竜西青鬼]$', surface)):
            #stop words
            #print(f'[{surface}] dropped (stop words)')
            pass
        elif(part.startswith('名詞') and not part.startswith('名詞,非自立')):
            if(re.match(r'^(こと|もの|www|http[s]?|[0-9]+)$', surface)):
                # not much meaningful
                #print(f'[{surface}] dropped (not much meaningful)')
                pass
            else:
                #print(f'[{surface}] added')
                words.append(surface)
        elif(part.startswith('動詞,自立') or part.startswith('形容詞,自立')):
            if(re.match(r'^(する|なる|言う|見る|思う|聞く|いう|やる|行く|いく|できる|出来る|ある|ない|いる|いない|http[s]?|[0-9]+)$', baseform)):
                # not much meaningful
                #print(f'[{baseform}] dropped (not much meaningful)')
                pass
            else:
                #print(f'[{baseform}] added')
                words.append(baseform)
        else:
            #print(f'[{baseform}] unknown !!!')
            pass

    print(words)
    return words

if __name__ == '__main__':
    import sys
    with open(sys.argv[1]) as f:
        for line in f:
            print('----------')
            tokenize(line)
