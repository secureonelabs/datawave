package datawave.query.iterator;

import datawave.data.normalizer.AbstractNormalizer;
import datawave.data.type.BaseType;

/**
 * Purely testing type to remove all commas from a string.
 */
public class CommaFreeType extends BaseType<String> {
    private static final long serialVersionUID = -8183805795495756459L;

    public CommaFreeType() {
        super(new CommaNormalizer());
    }

    public static class CommaNormalizer extends AbstractNormalizer<String> {
        private static final long serialVersionUID = -5731092165340522114L;

        @Override
        public String normalize(String s) {
            return s.replaceAll(",", "");
        }

        @Override
        public String normalizeDelegateType(String s) {
            return this.normalize(s);
        }

        @Override
        public String denormalize(String s) {
            return s;
        }

        @Override
        public String normalizeRegex(String s) {
            return s;
        }
    }
}
