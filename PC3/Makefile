JARS = jeromq-0.5.3.jar:sqlite-jdbc-3.46.0.0.jar:slf4j-api-2.0.13.jar:slf4j-simple-2.0.13.jar
CP = .:$(JARS)
SOURCES = $(wildcard *.java)

.PHONY: all check-jars compile run run-db run-all clean

all: run-all

check-jars:
	@test -f jeromq-0.5.3.jar || (echo "ERROR: falta jeromq-0.5.3.jar" && exit 1)
	@test -f sqlite-jdbc-3.46.0.0.jar || (echo "ERROR: falta sqlite-jdbc-3.46.0.0.jar" && exit 1)
	@test -f slf4j-api-2.0.13.jar || (echo "ERROR: falta slf4j-api-2.0.13.jar" && exit 1)
	@test -f slf4j-simple-2.0.13.jar || (echo "ERROR: falta slf4j-simple-2.0.13.jar" && exit 1)

compile: check-jars
	javac -cp "$(CP)" $(SOURCES)

run: run-db

run-db: compile
	java -cp "$(CP)" BaseDatosPrincipal

run-all: run-db

clean:
	rm -f *.class
