JARS = jeromq-0.5.3.jar
CP = .:$(JARS)
SOURCES = $(wildcard *.java)

.PHONY: all check-jars compile run run-broker run-sensores run-all clean

all: compile

check-jars:
	@test -f jeromq-0.5.3.jar || (echo "ERROR: falta jeromq-0.5.3.jar" && exit 1)

compile: check-jars
	javac -cp "$(CP)" $(SOURCES)

run: run-sensores

run-broker: compile
	java -cp "$(CP)" BrokerZMQ

run-sensores: compile
	java -cp "$(CP)" PC1_SensoresMain

run-all:
	@echo "Iniciando Broker y Sensores (todos) en terminales separadas..."
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
	eval "$$TERM_OPEN 'cd \"$(CURDIR)\"; java -cp \".:jeromq-0.5.3.jar\" BrokerZMQ; exec bash'"; \
	eval "$$TERM_OPEN 'cd \"$(CURDIR)\"; java -cp \".:jeromq-0.5.3.jar\" PC1_SensoresMain; exec bash'"

clean:
	rm -f *.class
