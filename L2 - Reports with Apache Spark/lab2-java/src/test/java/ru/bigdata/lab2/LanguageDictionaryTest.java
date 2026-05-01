package ru.bigdata.lab2;

import org.junit.jupiter.api.Test;
import ru.bigdata.lab2.model.LanguageAlias;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LanguageDictionaryTest {

    @Test
    void parsesAliasesAndAddsManualVariants() {
        Set<LanguageAlias> aliases = LanguageDictionary.parseAliases("Visual Basic .NET,https://en.wikipedia.org/wiki/Visual_Basic_.NET");
        Set<String> aliasValues = aliases.stream().map(LanguageAlias::getAlias).collect(Collectors.toSet());

        assertTrue(aliasValues.contains("visual-basic-.net"));
        assertTrue(aliasValues.contains("visual-basic"));
        assertTrue(aliasValues.contains("vb.net"));
    }

    @Test
    void addsGolangAliasForGo() {
        Set<LanguageAlias> aliases = LanguageDictionary.parseAliases("Go,https://en.wikipedia.org/wiki/Go_(programming_language)");
        Set<String> aliasValues = aliases.stream().map(LanguageAlias::getAlias).collect(Collectors.toSet());

        assertTrue(aliasValues.contains("go"));
        assertTrue(aliasValues.contains("golang"));
    }

    @Test
    void preservesSlashInsideWikipediaTitle() {
        Set<LanguageAlias> aliases = LanguageDictionary.parseAliases("PL/C,https://en.wikipedia.org/wiki/PL/C");
        Set<String> aliasValues = aliases.stream().map(LanguageAlias::getAlias).collect(Collectors.toSet());

        assertTrue(aliasValues.contains("pl/c"));
        assertFalse(aliasValues.contains("c"));
    }

    @Test
    void normalizesSwiftCanonicalName() {
        Set<LanguageAlias> aliases = LanguageDictionary.parseAliases("Swift (Apple programming language),https://en.wikipedia.org/wiki/Swift_(programming_language)");
        Set<String> canonicalNames = aliases.stream().map(LanguageAlias::getCanonicalName).collect(Collectors.toSet());

        assertEquals(Set.of("Swift"), canonicalNames);
    }

    @Test
    void prefersCleanerCanonicalNameWhenAliasDuplicatesExist() {
        assertEquals("Bash", LanguageDictionary.preferCanonicalName("Bash", "bash and"));
        assertEquals("C++", LanguageDictionary.preferCanonicalName("C++", "C++ – ISO/IEC 14882"));
    }
}
