JARS = jeromq-0.5.3.jar:sqlite-jdbc-3.46.0.0.jar:slf4j-api-2.0.13.jar:slf4j-simple-2.0.13.jar
# include parent dir so HmacUtil.class (compiled from ../HmacUtil.java) is found at runtime
CP = ..:.:$(JARS)
SOURCES = $(wildcard *.java) ../HmacUtil.java

.PHONY: all check-jars compile run run-analitica run-semaforos run-replica run-all clean

all: run-all

check-jars:
	@test -f jeromq-0.5.3.jar || (echo "ERROR: falta jeromq-0.5.3.jar" && exit 1)
	@test -f sqlite-jdbc-3.46.0.0.jar || (echo "ERROR: falta sqlite-jdbc-3.46.0.0.jar" && exit 1)
	@test -f slf4j-api-2.0.13.jar || (echo "ERROR: falta slf4j-api-2.0.13.jar" && exit 1)
	@test -f slf4j-simple-2.0.13.jar || (echo "ERROR: falta slf4j-simple-2.0.13.jar" && exit 1)

compile: check-jars
	javac -cp "$(CP)" $(SOURCES)

run: run-analitica

run-analitica: compile
	java -cp "$(CP)" ServicioAnalitica

run-semaforos: compile
	java -cp "$(CP)" ServicioControlSemaforos

run-replica: compile
	java -cp "$(CP)" BaseDatosReplica

run-all:
	@echo "Iniciando procesos de PC2 en terminales separadas..."
	@TERM_OPEN=''; \
	if command -v gnome-terminal >/dev/null 2>&1; then \
		TERM_OPEN='gnome-terminal -- bash -lc'; \
	elif command -v konsole >/dev/null 2>&1; then \
		TERM_OPEN='konsole -e bash -lc'; \
	elif command -v xfce4-terminal >/dev/null 2>&1; then \
		TERM_OPEN='xfce4-terminal --hold -e bash -lc'; \
	elif command -v xterm >/dev/null 2>&1; then \
		TERM_OPEN='xterm -hold -e bash -lc'; \
	else \
		echo "ERROR: no se encontró emulador de terminal (gnome-terminal/konsole/xfce4-terminal/xterm)."; \
		exit 1; \
	fi; \
	make compile; \
	eval "$$TERM_OPEN 'cd \"$(CURDIR)\"; java -cp \"$(CP)\" BaseDatosReplica; exec bash'"; \
	eval "$$TERM_OPEN 'cd \"$(CURDIR)\"; java -cp \"$(CP)\" ServicioAnalitica; exec bash'"; \
	eval "$$TERM_OPEN 'cd \"$(CURDIR)\"; java -cp \"$(CP)\" ServicioControlSemaforos; exec bash'"

clean:
	rm -f *.class
