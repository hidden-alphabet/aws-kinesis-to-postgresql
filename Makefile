build:
	mvn package

bundle:
	zip bundle.zip Makefile
	zip bundle.zip pom.xml
	zip -r bundle.zip src
