nextflow.enable.dsl = 2

params.word = 'necroflow'
params.n = 3
params.outdir = 'results'

process MAKE_SEED {
    input:
    val word

    output:
    path 'seed.txt'

    script:
    """
    echo "${word}" > seed.txt
    """
}

process TO_UPPER {
    input:
    path seed

    output:
    path 'upper.txt'

    script:
    """
    tr a-z A-Z < "${seed}" > upper.txt
    """
}

process TO_LOWER {
    input:
    path seed

    output:
    path 'lower.txt'

    script:
    """
    tr A-Z a-z < "${seed}" > lower.txt
    """
}

process REVERSE_IT {
    input:
    path seed

    output:
    path 'rev.txt'

    script:
    """
    rev "${seed}" > rev.txt
    """
}

process SORT_CHARS {
    input:
    path seed

    output:
    path 'sorted.txt'

    script:
    """
    grep -o . "${seed}" | sort > sorted.txt
    """
}

process ENCODE_ROT13 {
    input:
    path upper

    output:
    path 'rot13.txt'

    script:
    """
    tr A-Za-z N-ZA-Mn-za-m < "${upper}" > rot13.txt
    """
}

process REPEAT_WORD {
    input:
    path lower
    val n

    output:
    path 'rep.txt'

    script:
    """
    awk -v n="${n}" '{ for (i=0; i<n; i++) print }' "${lower}" > rep.txt
    """
}

process MERGE_CASES {
    input:
    path upper
    path lower

    output:
    path 'merged.txt'

    script:
    """
    paste "${upper}" "${lower}" > merged.txt
    """
}

process UNIQUE_CHARS {
    input:
    path sorted_chars

    output:
    path 'unique.txt'

    script:
    """
    uniq "${sorted_chars}" > unique.txt
    """
}

process COMBINE_ALL {
    input:
    path merged
    path rot13
    path repeated
    path reversed

    output:
    path 'combined.txt'

    script:
    """
    cat "${merged}" "${rot13}" "${repeated}" "${reversed}" > combined.txt
    """
}

process MAKE_STATS {
    publishDir params.outdir, mode: 'copy',
        saveAs: { name -> name == 'audit.txt' ? name : null }

    input:
    path combined
    path unique_chars

    output:
    path 'stats.txt', emit: stats
    path 'audit.txt', emit: audit

    script:
    """
    wc -c "${combined}" > stats.txt
    wc -l "${unique_chars}" > audit.txt
    """
}

process SHOUT_ROT {
    input:
    path rot13

    output:
    path 'upper_rot.txt'

    script:
    """
    tr a-z A-Z < "${rot13}" > upper_rot.txt
    """
}

process SORT_COMBINED {
    input:
    path combined

    output:
    path 'sorted_combined.txt'

    script:
    """
    sort "${combined}" > sorted_combined.txt
    """
}

process COUNT_LINES {
    input:
    path combined

    output:
    path 'lines.txt'

    script:
    """
    wc -l < "${combined}" > lines.txt
    """
}

process FINAL_MIX {
    input:
    path upper_rot
    path sorted_combined

    output:
    path 'final_mix.txt'

    script:
    """
    cat "${upper_rot}" "${sorted_combined}" > final_mix.txt
    """
}

process GRAND_SUMMARY {
    publishDir params.outdir, mode: 'copy'

    input:
    path stats
    path line_counts
    path final_mix

    output:
    path 'summary.txt'

    script:
    """
    cat "${stats}" "${line_counts}" "${final_mix}" > summary.txt
    """
}

workflow {
    seed = MAKE_SEED(params.word)

    upper = TO_UPPER(seed)
    lower = TO_LOWER(seed)
    reversed = REVERSE_IT(seed)
    sorted_chars = SORT_CHARS(seed)

    rot13 = ENCODE_ROT13(upper)
    repeated = REPEAT_WORD(lower, params.n)
    merged = MERGE_CASES(upper, lower)
    unique_chars = UNIQUE_CHARS(sorted_chars)

    combined = COMBINE_ALL(merged, rot13, repeated, reversed)
    MAKE_STATS(combined, unique_chars)

    upper_rot = SHOUT_ROT(rot13)
    sorted_combined = SORT_COMBINED(combined)
    line_counts = COUNT_LINES(combined)
    final_mix = FINAL_MIX(upper_rot, sorted_combined)

    GRAND_SUMMARY(MAKE_STATS.out.stats, line_counts, final_mix)
}
