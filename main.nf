#!/usr/bin/env nextflow

process sayHello {
    input:
    val greeting

    output:
    path "${greeting}.txt"

    script:
    """
    echo '${greeting} world!' > '${greeting}.txt'
    """
}

workflow {
    main:
    ch_greetings = channel.of('Bonjour', 'Ciao', 'Hello', 'Hola')
    sayHello(ch_greetings)

    // Workflow output definition (modern approach): assign the channel of
    // published files to a named output…
    publish:
    greetings = sayHello.out
}

// …and declare where that named output lands under outputDir.
output {
    greetings {
        path 'greetings'
    }
}
