# Builds the library and the end-to-end suite with ocamlfind alone, for
# machines without dune. (dune users: `dune build`; the two do not share a build directory.)
#
#   make            # build/brahmaputra.cmxa and build/manual_test.exe
#   make byte       # build/byte/brahmaputra.cma
#   make test HOST=127.0.0.1 PORT=9092

OCAMLFIND ?= ocamlfind
PACKAGES  := unix,threads.posix,zip
# Default warning set, with any warning it reports treated as an error.
WARN      ?= -warn-error +a
FLAGS     := -g -bin-annot -thread $(WARN)
MODULES   := protocol connection router assignor producer consumer group
SRC       := $(addprefix lib/,$(addsuffix .ml,$(MODULES)))
B         := build
HOST      ?= 127.0.0.1
PORT      ?= 9092

.PHONY: all lib byte test clean

all: lib $(B)/manual_test.exe

lib: $(B)/brahmaputra.cmxa

# Modules are packed into one `Brahmaputra` module, which is the same
# namespace dune's wrapped library gives (Brahmaputra.Producer, ...).
$(B)/brahmaputra.cmxa: $(SRC)
	@mkdir -p $(B)
	cp $(SRC) $(B)/
	cd $(B) && for m in $(MODULES); do \
	  $(OCAMLFIND) ocamlopt $(FLAGS) -package $(PACKAGES) -for-pack Brahmaputra -c $$m.ml || exit 1; \
	done
	cd $(B) && $(OCAMLFIND) ocamlopt $(FLAGS) -pack -o brahmaputra.cmx $(addsuffix .cmx,$(MODULES))
	cd $(B) && $(OCAMLFIND) ocamlopt $(FLAGS) -a -o brahmaputra.cmxa brahmaputra.cmx

byte: $(SRC)
	@mkdir -p $(B)/byte
	cp $(SRC) $(B)/byte/
	cd $(B)/byte && for m in $(MODULES); do \
	  $(OCAMLFIND) ocamlc $(FLAGS) -package $(PACKAGES) -for-pack Brahmaputra -c $$m.ml || exit 1; \
	done
	cd $(B)/byte && $(OCAMLFIND) ocamlc $(FLAGS) -pack -o brahmaputra.cmo $(addsuffix .cmo,$(MODULES))
	cd $(B)/byte && $(OCAMLFIND) ocamlc $(FLAGS) -a -o brahmaputra.cma brahmaputra.cmo

$(B)/manual_test.exe: $(B)/brahmaputra.cmxa test/manual_test.ml
	cp test/manual_test.ml $(B)/
	cd $(B) && $(OCAMLFIND) ocamlopt $(FLAGS) -package $(PACKAGES) -linkpkg \
	  brahmaputra.cmxa manual_test.ml -o manual_test.exe

test: $(B)/manual_test.exe
	$(B)/manual_test.exe $(HOST) $(PORT)

clean:
	rm -rf $(B)
