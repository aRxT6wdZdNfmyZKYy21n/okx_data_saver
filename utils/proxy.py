import codecs
import os
import os.path
import typing


class ProxyUtils(object):
    @staticmethod
    def get_socks5_proxy_urls(
            socks5_proxies_file_names: (
                typing.List[
                    str
                ]
            )
    ) -> (
            typing.List[
                typing.Optional[
                    str
                ]
            ]
    ):
        socks5_proxy_urls: (
            typing.List[
                typing.Optional[
                    str
                ]
            ]
        ) = []

        for socks5_proxies_file_name in (
                socks5_proxies_file_names
        ):
            with (
                    codecs.open(
                        os.path.join(
                            'data/',
                            socks5_proxies_file_name
                        )
                    )
            ) as proxies_file:
                socks5_proxy_urls.extend(
                    filter(
                        bool,

                        map(
                            str.strip,

                            proxies_file
                        )
                    )
                )

        # socks5_proxy_urls.append(
        #     None
        # )

        return (
            socks5_proxy_urls
        )
