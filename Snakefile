import os
import gzip


configfile: "config.yaml"


LANG1 = config["LANG1"]
LANG2 = config["LANG2"]
SRC_LANG = LANG1
TRG_LANG = LANG2

TRAIN_PREFIXES = config["TRAIN_PREFIXES"]

WORKFLOW = os.path.dirname(os.path.realpath(__file__))
TRANSIENT = '/tmp'

# can set an external moses/mgiza, but defaults to the ones submoduled
MOSES_HOME = config.get("MOSES_HOME", "./mosesdecoder")
MGIZA_HOME = config.get("MGIZA_HOME", "./mgiza/mgizapp/bin")

WORDTOK1 = f'{MOSES_HOME}/scripts/tokenizer/tokenizer.perl'
WORDTOK2 = f'{MOSES_HOME}/scripts/tokenizer/tokenizer.perl'
LOWERCASE = f"{MOSES_HOME}/scripts/tokenizer/lowercase.perl"

DIC = f"{LANG1}-{LANG2}.dic"

THREADS = config.get("THREADS", 1)


# Temporal directories for generated data
mgizaModelDir = f"{TRANSIENT}/{LANG1}_{LANG2}/tempgizamodel.{SRC_LANG}-{TRG_LANG}"
preprocCorpusDir = f"{TRANSIENT}/{LANG1}_{LANG2}/tempcorpuspreproc.{SRC_LANG}-{TRG_LANG}"


#################################################################
### RULES #######################################################

rule dic_generation_tokenize_file:
    input:
        expand("{trainPrefixes}.{{lang}}", trainPrefixes=TRAIN_PREFIXES),
    output:
        f"{preprocCorpusDir}/corpus.tok.{{lang}}",
    params:
        wordtok=lambda wildcards: WORDTOK1 if wildcards.lang == SRC_LANG else WORDTOK2
    shell:
        """
        mkdir -p {preprocCorpusDir}
        cat {input} \
            | sed -e \"s/&apos;/'/g\" -e 's/&quot;/\"/g' -e 's/&amp;/\&/g' \
            | {params.wordtok} > {output}
        """


rule dic_generation_lowercase:
    input:
        "{prefix}.tok.{lang}",
    output:
        "{prefix}.tok.low.{lang}",
    shell:
        """
        cat {input} | {LOWERCASE} > {output}
        """


rule dic_generation_clean:
    input:
        f"{{prefix}}.tok.low.{SRC_LANG}",
        f"{{prefix}}.tok.low.{TRG_LANG}",
    output:
        f"{{prefix}}.clean.{SRC_LANG}",
        f"{{prefix}}.clean.{TRG_LANG}",
    shell:
        """
        perl {MOSES_HOME}/scripts/training/clean-corpus-n.perl {wildcards.prefix}.tok.low \
            {SRC_LANG} {TRG_LANG} {wildcards.prefix}.clean 1 80 {wildcards.prefix}.lines-retained
        """


rule dic_generation_mkcls:
    input:
        f"{preprocCorpusDir}/corpus.clean.{{lang}}",
    output:
        f"{mgizaModelDir}/corpus.{{lang}}.vcb.classes",
    priority: 40
    shell:
        """
        {MGIZA_HOME}/mkcls -c50 -n2 -p{input} -V{output} opt 2> /dev/null > /dev/null
        """


rule dic_generation_plain2snt:
    input:
        l1=f"{preprocCorpusDir}/corpus.clean.{SRC_LANG}",
        l2=f"{preprocCorpusDir}/corpus.clean.{TRG_LANG}",
    output:
        snt_2_1=f"{mgizaModelDir}/corpus.{TRG_LANG}-{SRC_LANG}-int-train.snt",
        snt_1_2=f"{mgizaModelDir}/corpus.{SRC_LANG}-{TRG_LANG}-int-train.snt",
        vcb1=f"{mgizaModelDir}/corpus.{SRC_LANG}.vcb",
        vcb2=f"{mgizaModelDir}/corpus.{TRG_LANG}.vcb",
    priority: 40
    shell:
        """
        mkdir -p {mgizaModelDir}
        {MGIZA_HOME}/plain2snt {input.l1} {input.l2} 2> /dev/null > /dev/null
        mv {preprocCorpusDir}/corpus.clean.{SRC_LANG}_corpus.clean.{TRG_LANG}.snt {output.snt_2_1}
        mv {preprocCorpusDir}/corpus.clean.{TRG_LANG}_corpus.clean.{SRC_LANG}.snt {output.snt_1_2}
        cp {preprocCorpusDir}/corpus.clean.{SRC_LANG}.vcb {output.vcb1}
        cp {preprocCorpusDir}/corpus.clean.{TRG_LANG}.vcb {output.vcb2}
        """


rule dic_generation_snt2cooc:
    input:
        vcb1="{prefix}.{l1}.vcb",
        vcb2="{prefix}.{l2}.vcb",
        vcb1cls="{prefix}.{l1}.vcb.classes",
        vcb2cls="{prefix}.{l2}.vcb.classes",
        snt="{prefix}.{l2}-{l1}-int-train.snt",
    output:
        "{prefix}.{l2}-{l1}.cooc",
    shell:
        """
        {MGIZA_HOME}/snt2cooc {output} {input.vcb1} {input.vcb2} {input.snt} 2> /dev/null
        """


rule dic_generation_mgiza:
    input:
        vcb1="{prefix}.{l1}.vcb",
        vcb2="{prefix}.{l2}.vcb",
        snt="{prefix}.{l2}-{l1}-int-train.snt",
        cooc="{prefix}.{l2}-{l1}.cooc",
    output:
        "{prefix}.{l2}-{l1}.t3.final",
    shell:
        """
        {MGIZA_HOME}/mgiza -ncpus {THREADS} -CoocurrenceFile {input.cooc} -c {input.snt} \
            -m1 5 -m2 0 -m3 3 -m4 3 -mh 5 -m5 0 -model1dumpfrequency 1 -o {wildcards.prefix}.{wildcards.l2}-{wildcards.l1} \
            -s {input.vcb1} -t {input.vcb2} -emprobforempty 0.0 -probsmooth 1e-7 2> /dev/null > /dev/null
        """


rule dic_generation_filter_dics:
    input:
        "{prefix}.vcb",
    output:
        "{prefix}.filtered.vcb",
    shell:
        """
        cat {input} | egrep ' [^ ][^ ]+$' > {output}
        """


rule dic_generation_gzip_freq_dic:
    input:
        vcb1=f"{mgizaModelDir}/corpus.{SRC_LANG}.filtered.vcb",
        vcb2=f"{mgizaModelDir}/corpus.{TRG_LANG}.filtered.vcb",
    output:
        vcb1=f"{mgizaModelDir}/corpus.{SRC_LANG}.filtered.vcb.gz",
        vcb2=f"{mgizaModelDir}/corpus.{TRG_LANG}.filtered.vcb.gz",
    shell:
        """
        gzip -c {input.vcb1} > {output.vcb1}
        gzip -c {input.vcb2} > {output.vcb2}
        """


# Obtaining the harmonic probability of each pair of words in both directions and filtering out those with less than p=0.2; printing the dictionary
rule dic_generation_symmetrise_dic:
    input:
        vcb1=f"{mgizaModelDir}/corpus.{SRC_LANG}.filtered.vcb",
        vcb2=f"{mgizaModelDir}/corpus.{TRG_LANG}.filtered.vcb",
        t3_1=f"{mgizaModelDir}/corpus.{SRC_LANG}-{TRG_LANG}.t3.final",
        t3_2=f"{mgizaModelDir}/corpus.{TRG_LANG}-{SRC_LANG}.t3.final",
    output:
        expand("{dic}", dic=DIC),
    default_target: True,
    run:
        svocabulary = {}
        tvocabulary = {}
        svcb = open(input.vcb1, "r")
        tvcb = open(input.vcb2, "r")
        for line in svcb:
            item = line.strip().split(" ")
            svocabulary[item[0]] = item[1]

        for line in tvcb:
            item = line.strip().split(" ")
            tvocabulary[item[0]] = item[1]

        t3dic = {}
        t3s = open(input.t3_1, "r")
        t3t = open(input.t3_2, "r")
        for line in t3t:
            item = line.strip().split(" ")
            if item[1] in t3dic:
                t3dic[item[1]][item[0]] = item[2]
            else:
                t3dic[item[1]] = {}
                t3dic[item[1]][item[0]] = item[2]

        dic = open(output[0], "wt")
        dic.write(f"{SRC_LANG}\t{TRG_LANG}\n")
        for line in t3s:
            item = line.strip().split(" ")
            if item[0] in t3dic:
                if item[1] in t3dic[item[0]]:
                    value1 = float(t3dic[item[0]][item[1]])
                    value2 = float(item[2])
                    hmean = 2 / ((1 / value1) + (1 / value2))

                    if hmean > 0.1:
                        if item[1] in svocabulary and item[0] in tvocabulary:
                            word1 = svocabulary[item[1]]
                            word2 = tvocabulary[item[0]]
                            if word1.isalpha() or word2.isalpha():
                                dic.write("{0}\t{1}\n".format(word1, word2))
        svcb.close()
        tvcb.close()
        t3s.close()
        t3t.close()
        dic.close()
        os.sync()
