#include "api/TNTDatabase.h"
#include "misc/GlobalFactory.h"
#include <iostream>

using namespace ntse;
using namespace tnt;
using namespace std;

void short_usage(const char *prog) {
	cout << prog << " backupdir dbdir logdir" << endl;
	exit(1);
}


int main(int argc, char **argv) {
	// °ïÖú
	if (argc == 2 && (0 == strcmp(argv[1], "-h") || 0 == strcmp(argv[1], "--help"))) {
		short_usage(argv[0]);
	} else if (argc != 4) {
		short_usage(argv[0]);
	}

	try {
		GlobalFactory::getInstance();
		Tracer::init();
		
		TNTDatabase::restore(argv[1], argv[2], argv[3]);
		
		Tracer::exit();
		GlobalFactory::freeInstance();
	} catch (NtseException &e) {
		GlobalFactory::freeInstance();
		cerr << "Restore backup failed. " << e.getMessage() << endl;
		exit(-1);
	}

	cout << "Restore backup successful" << endl;
	return 0;
}