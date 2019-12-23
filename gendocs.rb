#!/usr/bin/env ruby

require 'asciidoctor'

docFiles = {
    "docs/README.adoc" => "README.adoc"
}

# For each documentation file, process all of the 'include' macros upfront since this is not supported by GitHub
docFiles.each {
    |source, destination|
    puts "Coalescing #{source} into #{destination}"

    doc = Asciidoctor.load_file source, safe: :unsafe, parse: false

    lines = doc.reader.read.gsub(/^include::(?=.*\[\]$)/m, '\\include::')

    File.open(destination, 'w') {|f| f.write lines }
}

