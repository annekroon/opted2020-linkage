import lxml
import numpy as np
import pandas as pd
import logging
import argparse
import os
from lxml import etree

def parse_kamervragen(path_to_data):
    count = 0
    error = 0
    results = []
    for filename in os.listdir(path_to_data):
        if not filename.endswith('.xml'): continue
        fullname = os.path.join(path_to_data, filename)
        count += 1
        results_d = {}
        try:
            root = lxml.etree.parse(fullname)
            questions = ' '.join([r.strip() for r in root.xpath('//vraag//text()')])
            answers = ' '.join([r.strip() for r in root.xpath('//antwoord//text()')])
            describ = ' '.join([r.strip() for r in root.xpath('//omschr//text() | //kamervraagomschrijving//text()')])
            date_send_in = ' '.join([ x.strip() for x in root.xpath('//datum//text()')[0].replace('(', '').replace(')', '').split()[1:]])
            date_received = ' '.join([ x.strip() for x in root.xpath('//datum//text()')[1].replace('(', '').replace(')', '').split()[1:]])
            name = ' '.join([r.strip() for r in root.xpath('//naam//text()')])
            questions_answers = ' '.join([r.strip() for r in root.xpath('//kamervragen//text()')])
        except:
            logger.warning(">>> ERROR in filename: {}".format(fullname))
            error += 1
            questions = "NaN"
            answers = "NaN"
            describ = "NaN"
            date_send_in = "NaN"
            date_received = "NaN"
            name = "NaN"
        results_d = {'questions': questions ,
                     'answers' : answers ,
                     'filename' : filename ,
                     'describ' : describ,
                     'date_send_in' : date_send_in,
                     'date_received' : date_received,
                     'name' : name }
        results.append(results_d)
    return results, count, error


def main(args):

	results, count, error = parse_kamervragen(args.data_path)
	print('Done! \n\nParsed {} kamervragen from the folder {}. {} errors were reported, meaning that these files could some how not be parsed'.format(count, args.data_path, error))

	df = pd.DataFrame.from_dict(results)
	print('Created dataframe!!\n\n')
	print(df.head(5))

	df.to_pickle('{}parliamentary_questions_parsed.pkl'.format(args.output))
	df.to_csv('{}parliamentary_questions_parsed.csv'.format(args.output))

if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s')
    logging.root.setLevel(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Parse parliamentary questions')
    parser.add_argument('--data_path', type=str, required=False, default='/Users/anne/Dropbox/kamervragen-xml/', help='Path to kamervragen')
    parser.add_argument('--output', type=str, required=False, default='/Users/anne/surfdrive/uva/projects/RPA_KeepingScore/data/', help='Path of output file (CSV / pickle)')
    args = parser.parse_args()

    print('Arguments:')
    print('data_path:', args.data_path)
    print('output:', args.output)
    print()

    main(args)
