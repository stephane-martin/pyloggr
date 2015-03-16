/**
 * Created by stef on 16/03/15.
 */
require.config({
    baseUrl: 'static',
	paths: {
		react: 'vendor/react/react',
		jquery: 'vendor/jquery/dist/jquery.min'
	},

	shim: {
		react: {
			exports: 'React'
		},

		jquery: {
			exports: '$'
		}

	}
});

