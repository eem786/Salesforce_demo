from sf_connection import ORG1, ORG2
import argparse
from multiprocessing import Pool

def get_org_metadata(connection):

    ''' This will return back the overall public metadata '''
    return connection._sf.describe()



def compare_orgs(production, sandbox):

    ''' Compare the objects based on the name between production and the sandbox '''

    production_objects = [ item['name'] for item in production['sobjects']]
    new_objects = [ item['name'] for item in sandbox['sobjects']
        if item['name'] not in production_objects ]

    if not new_objects:
        print 'No new Objects were created for this sandbox'
    else:
        print 'New objects that were created for this sandbox:'
        for new_object in new_objects:
            print "  {}".format(new_object)

    return production_objects

def compare_newattributes(args):

    production = args['production']
    sandbox = args['sandbox']
    obj_name = args['obj_name']
    attribute = args['attribute']

    production_recordtypes = [ item['name'] for item in production[attribute]]
    new_recordtypes = [ item['name'] for item in sandbox[attribute]
        if item['name'] not in production_recordtypes]

    return {'name': obj_name, 'new_attributes': new_recordtypes }


def main(password):

    production = ORG1(password)
    sandbox = ORG2(password)

    production_org = get_org_metadata(production)
    sandbox_org = get_org_metadata(sandbox)

    # Check for new objects that are created
    print "\n ** Creating report for moving to production ** \n"
    print "Checking for new objects"
    print "---------------------------------"
    production_objects = compare_orgs(production_org, sandbox_org)

    # Check for any new recordTypes, fields, pageLayouts, etc
    production_fields = {obj_name: getattr(production._sf, obj_name).describe() for obj_name in production_objects }
    sandbox_fields = {obj_name: getattr(sandbox._sf, obj_name).describe() for obj_name in production_objects }

    for attribute in ['fields', 'recordTypeInfos']:
        iterlist = [{'production': production_fields[obj_name],
                     'sandbox': sandbox_fields[obj_name],
                     'obj_name': obj_name,
                     'attribute': attribute }
                    for obj_name in production_objects ]
        pool = Pool(processes=4)
        print "\nChecking for new {} on existing objects".format(attribute)
        print "--------------------------------------------------"
        for output in pool.map(compare_newattributes, iterlist):
            if output['new_attributes']:
                print 'New {} found for: {}'.format(attribute, output['name'])
                for new_recordtype in output['new_attributes']:
                    print "  {}".format(new_recordtype)




if __name__ == "__main__":

    parser = argparse.ArgumentParser('Tool to help diff Salesforce instances')
    parser.add_argument('-p', '--password', help='the password for the instances -- Assumes that the password is the same cross orgs', default='gvIVwCD6v8')
    args = parser.parse_args()

    main(args.password)