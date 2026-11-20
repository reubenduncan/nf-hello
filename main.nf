#!/usr/bin/env nextflow

process sayHello {
    input:
    val greeting
    path input_file
    val recipient
    val run_number
    val uppercase
    val tone

    output:
    path "${greeting}.txt"

    script:
    def punctuation = [friendly: '!', formal: '.', curious: '?'][tone]
    def message = "${greeting} ${recipient}${punctuation}"
    if (uppercase) {
        message = message.toUpperCase()
    }
    def shellQuote = { value -> "'" + value.toString().replace("'", "'\\''") + "'" }

    """
    {
        printf '%s\\n' ${shellQuote(message)}
        printf 'Run number: %s\\n' ${shellQuote(run_number)}
        printf 'Input text:\\n'
        cat -- ${shellQuote(input_file)}
    } > '${greeting}.txt'
    """
}

workflow {
    main:
    ch_greetings = channel.of('Bonjour', 'Ciao', 'Hello', 'Hola')
    ch_input_file = channel.value(file(params.input_file ?: "${projectDir}/assets/input.txt"))
    ch_recipient = channel.value(params.recipient)
    ch_run_number = channel.value(params.run_number)
    ch_uppercase = channel.value(params.uppercase)
    ch_tone = channel.value(params.tone)

    sayHello(ch_greetings, ch_input_file, ch_recipient, ch_run_number, ch_uppercase, ch_tone)

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
